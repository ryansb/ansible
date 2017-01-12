from nose.plugins.skip import SkipTest
import pytest
import os

try:
    import boto3
    import botocore
    import placebo
except ImportError:
    raise SkipTest("test_ec2_asg.py requires the `boto3`, `botocore`, and `placebo` modules")


from ansible.modules.cloud.amazon import ec2_asg as asg_module


@pytest.fixture(scope='module')
def basic_launch_config():
    if not os.getenv('PLACEBO_RECORD'):
        yield 'pytest_basic_lc'
        return

    # use a *non recording* session to make the launch config
    # since that's a prereq of the ec2_asg module, and isn't what
    # we're testing.
    asg = boto3.client('autoscaling')
    asg.create_launch_configuration(
        LaunchConfigurationName='pytest_basic_lc',
        ImageId='ami-9be6f38c', # Amazon Linux 2016.09 AMI, can be any valid AMI
        SecurityGroups=[],
        UserData='#!/bin/bash\necho hello world',
        InstanceType='t2.micro',
        InstanceMonitoring={'Enabled': False},
        AssociatePublicIpAddress=True
    )

    yield 'pytest_basic_lc'

    try:
        asg.delete_launch_configuration(LaunchConfigurationName='pytest_basic_lc')
    except botocore.exceptions.ClientError as e:
        if 'not found' in e.message:
            pass
        else:
            raise


@pytest.fixture
def placeboify(request, monkeypatch):
    session = boto3.Session()
    import os

    recordings_path = os.path.join(
        request.fspath.dirname,
        'placebo_recordings',
        request.fspath.basename.replace('.py', ''),
        request.function.func_name
    ).replace('test_', '')

    try:
        # make sure the directory for placebo test recordings is available
        os.makedirs(recordings_path)
    except OSError as e:
        if e.errno != os.errno.EEXIST:
            raise

    pill = placebo.attach(session, data_path=recordings_path)
    if os.getenv('PLACEBO_RECORD'):
        pill.record()
    else:
        pill.playback()

    def boto3_middleman_connection(module, conn_type, resource, region, **kwargs):
        if conn_type != 'client':
            raise ValueError('Mocker only supports client, not %s' % conn_type)
        return session.client(resource, region_name=region)

    import ansible.module_utils.ec2
    monkeypatch.setattr(
        ansible.module_utils.ec2,
        'boto3_conn',
        boto3_middleman_connection,
    )
    yield session
    # tear down
    pill.stop()


class FailJSON(Exception):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        super(FailJSON, self).__init__(kwargs.get('msg'))


class ExitJSON(Exception):
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        super(ExitJSON, self).__init__(kwargs.get('msg'))

class FakeModule(object):
    params = None
    def __init__(self, name, launch_config_name, min_size, max_size,
            desired_capacity, **kwargs):
        self.params = dict(
            load_balancers=[],
            target_group_arns=[],
            placement_group=None,
            availability_zones=['us-east-1c', 'us-east-1d'],
            vpc_zone_identifier=[],
            replace_batch_size=1,
            replace_all_instances=False,
            replace_instances=[],
            lc_check=True,
            wait_timeout=300,
            state='present',
            tags=[],
            health_check_period=300,
            health_check_type='EC2',
            default_cooldown=300,
            wait_for_instances=True,
            termination_policies=[],
            notification_topic=None,
            notification_types=[
                'autoscaling:EC2_INSTANCE_LAUNCH',
                'autoscaling:EC2_INSTANCE_LAUNCH_ERROR',
                'autoscaling:EC2_INSTANCE_TERMINATE',
                'autoscaling:EC2_INSTANCE_TERMINATE_ERROR'
            ],
            suspend_processes=[]
        )

        self.params.update(name=name,
                launch_config_name=launch_config_name,
                min_size=min_size,
                max_size=max_size,
                desired_capacity=desired_capacity,
                **kwargs)

    def exit_json(self, *args, **kwargs):
        return ExitJSON(*args, **kwargs)

    def fail_json(self, *args, **kwargs):
        raise FailJSON(*args, **kwargs)

    def update(self, **kwargs):
        self.params.update(kwargs)

def test_create_with_nonexistent_launch_config(placeboify):
    connection = placeboify.client('autoscaling')
    module = FakeModule('test-asg-created', None, min_size=0, max_size=0, desired_capacity=0)
    with pytest.raises(FailJSON) as excinfo:
        asg_module.create_autoscaling_group(connection, module)
    excinfo.match('^Missing required arguments .* launch_config_name')

def test_create_with_launch_config(placeboify, basic_launch_config):
    connection = placeboify.client('autoscaling')
    module = FakeModule(name='test-asg-created',
            launch_config_name=basic_launch_config,
            min_size=0, max_size=0, desired_capacity=0,
            availability_zones=[])
    with pytest.raises(ExitJSON) as excinfo:
        asg_module.create_autoscaling_group(connection, module)
    assert excinfo.value.kwargs.changed is True
