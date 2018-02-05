#!/usr/bin/python
# Copyright: Ansible Project
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)

from __future__ import absolute_import, division, print_function
__metaclass__ = type


ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = '''
---
module: ec2_instance
short_description: Create & manage EC2 instances
description:
    - Gather facts about ec2 instances in AWS
version_added: "2.5"
author:
  - Ryan Scott Brown, @ryansb
requirements: [ "boto3", "botocore" ]
options:
  instance_ids:
    description:
      - If you specify one or more instance IDs, only instances that have the specified IDs are returned.
    required: false
  state:
    description:
    - Goal state for the instances
    choices: [present, terminated, running, started, stopped, restarted, rebooted, absent]
    default: present
  wait:
    description:
    - Whether or not to wait for the desired state (use wait_timeout to customize this)
    default: true
  wait_timeout:
    description:
    - How long to wait (in seconds) for the instance to finish booting/terminating
    default: 600
  instance_type:
    description:
      - Instance type to use for the instance, see U(http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-types.html)
      - Only required when instance is not already present
    required: true
    default: t2.micro
  user_data:
    description:
      - Opaque blob of data which is made available to the ec2 instance
    required: false
  tags:
    description:
      - A hash/dictionary of tags to add to the new instance or to add/remove from an existing one.
  purge_tags:
    description:
    - Delete any tags not specified in the task that are on the instance.
    - This means you have to specify all the desired tags on each task affecting an instance.
    default: false
  image_id:
    description:
       - I(ami) ID to use for the instance
    required: true
  vpc_subnet_id:
    description:
      - The subnet ID in which to launch the instance (VPC)
      - If none is provided, ec2_instance will chose the default zone of the default VPC
  assign_public_ip:
    description:
      - When provisioning within vpc, assign a public IP address. If not specified, the subnet default will be used.
      - This cannot be changed after an instance is created.
  termination_protection:
    version_added: "2.0"
    description:
      - Whether to enable termination protection.
      - This module will not terminate an instance with termination protection active, it must be turned off first.
    default: false
  ebs_optimized:
    description:
      - Whether instance is should use optimized EBS volumes, see U(http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/EBSOptimized.html)
  filters:
    description:
      - A dict of filters to apply when deciding whether existing instances match and should be altered. Each dict item
        consists of a filter key and a filter value. See
        U(http://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_DescribeInstances.html)
        for possible filters. Filter names and values are case sensitive.
      - By default, instances are filtered for counting by their "Name" tag, base AMI, state (running, by default), and
        subnet ID. Any queryable filter can be used. Good candidates are: specific tags, SSH keys, or security groups.
    required: false
    default: {"tag:Name": "<provided-Name-attribute>", "subnet-id": "<provided-or-default subnet>"}
  instance_role:
    description:
    - The ARN or name of an EC2-enabled instance role to be used. If a name is not provided in the format
      arn:aws:iam::... then the ListInstanceProfiles permission must also be granted.
      U(https://docs.aws.amazon.com/IAM/latest/APIReference/API_ListInstanceProfiles.html) If no full ARN is provided,
      the role with a matching name will be used from the active AWS account.

extends_documentation_fragment:
    - aws
    - ec2
'''

EXAMPLES = '''
# Note: These examples do not set authentication details, see the AWS Guide for details.

# Terminate every instance in a region. Use with caution.
- ec2_instance_facts:
    state: absent
    filters:
      instance-state-name: running

# Gather facts about all instances in AZ ap-southeast-2a
- ec2_instance_facts:
    filters:
      availability-zone: ap-southeast-2a

# Gather facts about a particular instance using ID
- ec2_instance_facts:
    instance_ids:
      - i-12345678

# Gather facts about any instance with a tag key Name and value Example
- ec2_instance_facts:
    filters:
      "tag:Name": Example
'''

RETURN = '''
instances:
    description: a list of ec2 instances
    returned: always
    type: complex
    contains:
        ami_launch_index:
            description: The AMI launch index, which can be used to find this instance in the launch group.
            returned: always
            type: int
            sample: 0
        architecture:
            description: The architecture of the image
            returned: always
            type: string
            sample: x86_64
        block_device_mappings:
            description: Any block device mapping entries for the instance.
            returned: always
            type: complex
            contains:
                device_name:
                    description: The device name exposed to the instance (for example, /dev/sdh or xvdh).
                    returned: always
                    type: string
                    sample: /dev/sdh
                ebs:
                    description: Parameters used to automatically set up EBS volumes when the instance is launched.
                    returned: always
                    type: complex
                    contains:
                        attach_time:
                            description: The time stamp when the attachment initiated.
                            returned: always
                            type: string
                            sample: "2017-03-23T22:51:24+00:00"
                        delete_on_termination:
                            description: Indicates whether the volume is deleted on instance termination.
                            returned: always
                            type: bool
                            sample: true
                        status:
                            description: The attachment state.
                            returned: always
                            type: string
                            sample: attached
                        volume_id:
                            description: The ID of the EBS volume
                            returned: always
                            type: string
                            sample: vol-12345678
        client_token:
            description: The idempotency token you provided when you launched the instance, if applicable.
            returned: always
            type: string
            sample: mytoken
        ebs_optimized:
            description: Indicates whether the instance is optimized for EBS I/O.
            returned: always
            type: bool
            sample: false
        hypervisor:
            description: The hypervisor type of the instance.
            returned: always
            type: string
            sample: xen
        iam_instance_profile:
            description: The IAM instance profile associated with the instance, if applicable.
            returned: always
            type: complex
            contains:
                arn:
                    description: The Amazon Resource Name (ARN) of the instance profile.
                    returned: always
                    type: string
                    sample: "arn:aws:iam::000012345678:instance-profile/myprofile"
                id:
                    description: The ID of the instance profile
                    returned: always
                    type: string
                    sample: JFJ397FDG400FG9FD1N
        image_id:
            description: The ID of the AMI used to launch the instance.
            returned: always
            type: string
            sample: ami-0011223344
        instance_id:
            description: The ID of the instance.
            returned: always
            type: string
            sample: i-012345678
        instance_type:
            description: The instance type size of the running instance.
            returned: always
            type: string
            sample: t2.micro
        key_name:
            description: The name of the key pair, if this instance was launched with an associated key pair.
            returned: always
            type: string
            sample: my-key
        launch_time:
            description: The time the instance was launched.
            returned: always
            type: string
            sample: "2017-03-23T22:51:24+00:00"
        monitoring:
            description: The monitoring for the instance.
            returned: always
            type: complex
            contains:
                state:
                    description: Indicates whether detailed monitoring is enabled. Otherwise, basic monitoring is enabled.
                    returned: always
                    type: string
                    sample: disabled
        network_interfaces:
            description: One or more network interfaces for the instance.
            returned: always
            type: complex
            contains:
                association:
                    description: The association information for an Elastic IPv4 associated with the network interface.
                    returned: always
                    type: complex
                    contains:
                        ip_owner_id:
                            description: The ID of the owner of the Elastic IP address.
                            returned: always
                            type: string
                            sample: amazon
                        public_dns_name:
                            description: The public DNS name.
                            returned: always
                            type: string
                            sample: ""
                        public_ip:
                            description: The public IP address or Elastic IP address bound to the network interface.
                            returned: always
                            type: string
                            sample: 1.2.3.4
                attachment:
                    description: The network interface attachment.
                    returned: always
                    type: complex
                    contains:
                        attach_time:
                            description: The time stamp when the attachment initiated.
                            returned: always
                            type: string
                            sample: "2017-03-23T22:51:24+00:00"
                        attachment_id:
                            description: The ID of the network interface attachment.
                            returned: always
                            type: string
                            sample: eni-attach-3aff3f
                        delete_on_termination:
                            description: Indicates whether the network interface is deleted when the instance is terminated.
                            returned: always
                            type: bool
                            sample: true
                        device_index:
                            description: The index of the device on the instance for the network interface attachment.
                            returned: always
                            type: int
                            sample: 0
                        status:
                            description: The attachment state.
                            returned: always
                            type: string
                            sample: attached
                description:
                    description: The description.
                    returned: always
                    type: string
                    sample: My interface
                groups:
                    description: One or more security groups.
                    returned: always
                    type: complex
                    contains:
                        - group_id:
                              description: The ID of the security group.
                              returned: always
                              type: string
                              sample: sg-abcdef12
                          group_name:
                              description: The name of the security group.
                              returned: always
                              type: string
                              sample: mygroup
                ipv6_addresses:
                    description: One or more IPv6 addresses associated with the network interface.
                    returned: always
                    type: complex
                    contains:
                        - ipv6_address:
                              description: The IPv6 address.
                              returned: always
                              type: string
                              sample: "2001:0db8:85a3:0000:0000:8a2e:0370:7334"
                mac_address:
                    description: The MAC address.
                    returned: always
                    type: string
                    sample: "00:11:22:33:44:55"
                network_interface_id:
                    description: The ID of the network interface.
                    returned: always
                    type: string
                    sample: eni-01234567
                owner_id:
                    description: The AWS account ID of the owner of the network interface.
                    returned: always
                    type: string
                    sample: 01234567890
                private_ip_address:
                    description: The IPv4 address of the network interface within the subnet.
                    returned: always
                    type: string
                    sample: 10.0.0.1
                private_ip_addresses:
                    description: The private IPv4 addresses associated with the network interface.
                    returned: always
                    type: complex
                    contains:
                        - association:
                              description: The association information for an Elastic IP address (IPv4) associated with the network interface.
                              returned: always
                              type: complex
                              contains:
                                  ip_owner_id:
                                      description: The ID of the owner of the Elastic IP address.
                                      returned: always
                                      type: string
                                      sample: amazon
                                  public_dns_name:
                                      description: The public DNS name.
                                      returned: always
                                      type: string
                                      sample: ""
                                  public_ip:
                                      description: The public IP address or Elastic IP address bound to the network interface.
                                      returned: always
                                      type: string
                                      sample: 1.2.3.4
                          primary:
                              description: Indicates whether this IPv4 address is the primary private IP address of the network interface.
                              returned: always
                              type: bool
                              sample: true
                          private_ip_address:
                              description: The private IPv4 address of the network interface.
                              returned: always
                              type: string
                              sample: 10.0.0.1
                source_dest_check:
                    description: Indicates whether source/destination checking is enabled.
                    returned: always
                    type: bool
                    sample: true
                status:
                    description: The status of the network interface.
                    returned: always
                    type: string
                    sample: in-use
                subnet_id:
                    description: The ID of the subnet for the network interface.
                    returned: always
                    type: string
                    sample: subnet-0123456
                vpc_id:
                    description: The ID of the VPC for the network interface.
                    returned: always
                    type: string
                    sample: vpc-0123456
        placement:
            description: The location where the instance launched, if applicable.
            returned: always
            type: complex
            contains:
                availability_zone:
                    description: The Availability Zone of the instance.
                    returned: always
                    type: string
                    sample: ap-southeast-2a
                group_name:
                    description: The name of the placement group the instance is in (for cluster compute instances).
                    returned: always
                    type: string
                    sample: ""
                tenancy:
                    description: The tenancy of the instance (if the instance is running in a VPC).
                    returned: always
                    type: string
                    sample: default
        private_dns_name:
            description: The private DNS name.
            returned: always
            type: string
            sample: ip-10-0-0-1.ap-southeast-2.compute.internal
        private_ip_address:
            description: The IPv4 address of the network interface within the subnet.
            returned: always
            type: string
            sample: 10.0.0.1
        product_codes:
            description: One or more product codes.
            returned: always
            type: complex
            contains:
                - product_code_id:
                      description: The product code.
                      returned: always
                      type: string
                      sample: aw0evgkw8ef3n2498gndfgasdfsd5cce
                  product_code_type:
                      description: The type of product code.
                      returned: always
                      type: string
                      sample: marketplace
        public_dns_name:
            description: The public DNS name assigned to the instance.
            returned: always
            type: string
            sample:
        public_ip_address:
            description: The public IPv4 address assigned to the instance
            returned: always
            type: string
            sample: 52.0.0.1
        root_device_name:
            description: The device name of the root device
            returned: always
            type: string
            sample: /dev/sda1
        root_device_type:
            description: The type of root device used by the AMI.
            returned: always
            type: string
            sample: ebs
        security_groups:
            description: One or more security groups for the instance.
            returned: always
            type: complex
            contains:
                - group_id:
                      description: The ID of the security group.
                      returned: always
                      type: string
                      sample: sg-0123456
                - group_name:
                      description: The name of the security group.
                      returned: always
                      type: string
                      sample: my-security-group
        source_dest_check:
            description: Indicates whether source/destination checking is enabled.
            returned: always
            type: bool
            sample: true
        state:
            description: The current state of the instance.
            returned: always
            type: complex
            contains:
                code:
                    description: The low byte represents the state.
                    returned: always
                    type: int
                    sample: 16
                name:
                    description: The name of the state.
                    returned: always
                    type: string
                    sample: running
        state_transition_reason:
            description: The reason for the most recent state transition.
            returned: always
            type: string
            sample:
        subnet_id:
            description: The ID of the subnet in which the instance is running.
            returned: always
            type: string
            sample: subnet-00abcdef
        tags:
            description: Any tags assigned to the instance.
            returned: always
            type: dict
            sample:
        virtualization_type:
            description: The type of virtualization of the AMI.
            returned: always
            type: string
            sample: hvm
        vpc_id:
            description: The ID of the VPC the instance is in.
            returned: always
            type: dict
            sample: vpc-0011223344
'''

import re
import uuid
import string
import textwrap
from collections import namedtuple

try:
    import boto3
    import botocore.exceptions
except ImportError:
    pass

from ansible.module_utils.six import text_type
from ansible.module_utils._text import to_bytes, to_native
import ansible.module_utils.ec2 as ec2_utils
from ansible.module_utils.ec2 import (boto3_conn,
                                      ec2_argument_spec,
                                      get_aws_connection_info,
                                      AWSRetry,
                                      ansible_dict_to_boto3_filter_list,
                                      compare_aws_tags,
                                      boto3_tag_list_to_ansible_dict,
                                      ansible_dict_to_boto3_tag_list,
                                      camel_dict_to_snake_dict)

from ansible.module_utils.aws.core import AnsibleAWSModule, HAS_BOTO3

module = None


def tower_callback_script(tower_conf, windows=False, passwd=None):
    script_url = 'https://raw.githubusercontent.com/ansible/ansible/devel/examples/scripts/ConfigureRemotingForAnsible.ps1'
    if windows and passwd is not None:
        script_tpl = """<powershell>
        $admin = [adsi]("WinNT://./administrator, user")
        $admin.PSBase.Invoke("SetPassword", "{PASS}")
        Invoke-Expression ((New-Object System.Net.Webclient).DownloadString('{SCRIPT}'))
        </powershell>
        """
        return to_native(textwrap.dedent(script_tpl).format(PASS=passwd, SCRIPT=script_url))
    elif windows and passwd is None:
        script_tpl = """<powershell>
        $admin = [adsi]("WinNT://./administrator, user")
        Invoke-Expression ((New-Object System.Net.Webclient).DownloadString('{SCRIPT}'))
        </powershell>
        """
        return to_native(textwrap.dedent(script_tpl).format(PASS=passwd, SCRIPT=script_url))
    elif not windows:
        tpl = string.Template(textwrap.dedent("""
        #!/bin/bash
        exec > /tmp/tower_callback.log 2>&1
        set -x

        retry_attempts=10
        attempt=0
        while [[ $attempt -lt $retry_attempts ]]
        do
          status_code=`curl -k -s -i \
                  --data "host_config_key=${host_config_key}" \
                  https://${tower_address}/api/v1/job_templates/${template_id}/callback/ \
                  | head -n 1 \
                  | awk '{print $2}'`
          if [[ $status_code == 202 ]]
            then
            exit 0
          fi
          attempt=$(( attempt + 1 ))
          echo "$${status_code} received... retrying in 1 minute. (Attempt $${attempt})"
          sleep 60
        done
        exit 1
        """))
        return tpl.safe_substitute(tower_address=tower_conf['tower_address'],
                                   template_id=tower_conf['job_template_id'],
                                   host_config_key=tower_conf['host_config_key'])
    raise NotImplementedError("Only windows with remote-prep or non-windows with tower job callback supported so far.")


def build_network_spec(params, ec2=None):
    """
    Returns list of interfaces [complex]
    Interface type: {
        'AssociatePublicIpAddress': True|False,
        'DeleteOnTermination': True|False,
        'Description': 'string',
        'DeviceIndex': 123,
        'Groups': [
            'string',
        ],
        'Ipv6AddressCount': 123,
        'Ipv6Addresses': [
            {
                'Ipv6Address': 'string'
            },
        ],
        'NetworkInterfaceId': 'string',
        'PrivateIpAddress': 'string',
        'PrivateIpAddresses': [
            {
                'Primary': True|False,
                'PrivateIpAddress': 'string'
            },
        ],
        'SecondaryPrivateIpAddressCount': 123,
        'SubnetId': 'string'
    },
    """
    if ec2 is None:
        ec2 = module.client('ec2')

    interfaces = []
    network = params.get('network') or {}
    if network or not params.get('network_interfaces'):
        # they only specified one interface
        spec = {
            'DeviceIndex': 0,
        }
        if network.get('source_dest_check') is not None:
            raise Exception("Uhoh - srcdest not supported")
            # TODO special setting via ec2-attrs
        if network.get('assign_public_ip') is not None:
            spec['AssociatePublicIpAddress'] = network['assign_public_ip']

        if params.get('vpc_subnet_id'):
            spec['SubnetId'] = params['vpc_subnet_id']
        else:
            default_vpc = get_default_vpc(ec2)
            if default_vpc is None:
                raise ValueError("You must include a VPC subnet ID to create an instance")
            else:
                sub = get_default_subnet(ec2, default_vpc)
                spec['SubnetId'] = sub['SubnetId']

        if network.get('private_ip_address'):
            spec['PrivateIpAddress'] = network['private_ip_address']
        # TODO more special snowflake network things
        return [spec]

    # handle list of `network_interfaces` options
    for idx, interface_params in enumerate(params.get('network_interfaces', [])):
        spec = {
            'DeviceIndex': idx,
        }

        if isinstance(interface_params, text_type):
            # naive case where user gave
            # network_interfaces: [eni-1234, eni-4567, ....]
            # put into normal data structure so we don't dupe code
            interface_params = {'eni_id': interface_params}

        spec['DeleteOnTermination'] = interface_params.get('delete_on_termination', True)

        if interface_params.get('ipv6_addresses'):
            spec['Ipv6Addresses'] = [{'Ipv6Address': a} for a in interface_params.get('ipv6_addresses', [])]
            spec['Ipv6AddressCount'] = len(spec['Ipv6Addresses'])

        if interface_params.get('description'):
            spec['Description'] = interface_params.get('description')

        spec['SubnetId'] = interface_params.get('subnet_id', params.get('vpc_subnet_id'))
        if not spec['SubnetId']:
            # TODO grab a subnet from default VPC
            raise ValueError('Failed to assign subnet to interface {0}'.format(interface_params))

        if interface_params.get('eni_id') is not None:
            spec['NetworkInterfaceId'] = interface_params['eni_id']
        interfaces.append(spec)
    return interfaces


def warn_if_public_ip_assignment_changed(instance):
    # This is a non-modifiable attribute.
    assign_public_ip = module.params.get('assign_public_ip')

    # Check that public ip assignment is the same and warn if not
    public_dns_name = instance.get('PublicDnsName')
    if (public_dns_name and not assign_public_ip) or (assign_public_ip and not public_dns_name):
        module.warn(
            "Unable to modify public ip assignment to {0} for instance {1}. "
            "Whether or not to assign a public IP is determined during instance creation.".format(
                assign_public_ip, instance['InstanceId']))


def discover_security_groups(group, groups, parent_vpc_id, ec2=None):
    if ec2 is None:
        ec2 = module.client('ec2')
    vpc = {
        'Name': 'vpc-id',
        'Values': [parent_vpc_id]
    }

    # because filter lists are AND in the security groups API,
    # make two separate requests for groups by ID and by name
    id_filters = [vpc]
    name_filters = [vpc]

    if group:
        name_filters.append(
            dict(
                Name='group-name',
                Values=[group]
            )
        )
        if group.startswith('sg-'):
            id_filters.append(
                dict(
                    Name='group-id',
                    Values=[group]
                )
            )
    if groups:
        name_filters.append(
            dict(
                Name='group-name',
                Values=groups
            )
        )
        if [g for g in groups if g.startswith('sg-')]:
            id_filters.append(
                dict(
                    Name='group-id',
                    Values=[g for g in groups if g.startswith('sg-')]
                )
            )

    found_groups = []
    for f_set in (id_filters, name_filters):
        if len(f_set) > 1:
            found_groups.extend(ec2.get_paginator(
                'describe_security_groups'
            ).paginate(
                Filters=f_set
            ).search('SecurityGroups[]'))
    return list({g['GroupId']: g for g in found_groups}.values())


def build_top_level_options(params):
    spec = {}
    if params.get('image_id'):
        spec['ImageId'] = params['image_id']
    elif isinstance(params.get('image'), dict):
        image = params.get('image', {})
        spec['ImageId'] = image.get('id')
        if 'ramdisk' in image:
            spec['RamdiskId'] = image['ramdisk']
        if 'kernel' in image:
            spec['KernelId'] = image['kernel']
    if not spec.get('ImageId') and not params.get('launch_template'):
        raise ValueError("You must include an image_id or image.id parameter to create an instance")

    if params.get('user_data') is not None:
        spec['UserData'] = to_native(params.get('user_data'))
    elif params.get('tower_callback') is not None:
        spec['UserData'] = tower_callback_script(
            tower_conf=params.get('tower_callback'),
            windows=params.get('tower_callback').get('windows', False),
            passwd=params.get('tower_callback').get('set_password'),
        )

    if params.get('detailed_monitoring', False):
        spec['Monitoring'] = {'Enabled': True}
    if params.get('instance_initiated_shutdown_behavior'):
        spec['InstanceInitiatedShutdownBehavior'] = params.get('instance_initiated_shutdown_behavior')
    if params.get('termination_protection') is not None:
        spec['DisableApiTermination'] = params.get('termination_protection')
    return spec


def build_instance_tags(params, propagate_tags_to_volumes=True):
    # TODO read existing tags
    # TODO distinguish between tags=None and tags={} for clearing tags vs. not changing tags
    tags = params.get('tags', {})
    if params.get('name') is not None:
        if tags is None:
            tags = {}
        tags['Name'] = params.get('name')
    return [
        {
            'ResourceType': 'volume',
            'Tags': ansible_dict_to_boto3_tag_list(tags),
        },
        {
            'ResourceType': 'instance',
            'Tags': ansible_dict_to_boto3_tag_list(tags),
        },
    ]


def build_run_instance_spec(params, ec2=None):
    if ec2 is None:
        ec2 = module.client('ec2')

    spec = dict(
        ClientToken=uuid.uuid4().hex,
        MaxCount=1,
        MinCount=1,
    )
    # network parameters
    spec['NetworkInterfaces'] = build_network_spec(params, ec2)
    spec.update(**build_top_level_options(params))
    spec['TagSpecifications'] = build_instance_tags(params)

    # IAM profile
    if params.get('iam_profile'):
        spec['IamInstanceProfile'] = dict(Arn=determine_iam_role(params.get('iam_profile')))

    spec['InstanceType'] = params['instance_type']
    return spec


def await_instances(ids, state='OK'):
    if not module.params.get('wait', True):
        # the user asked not to wait for anything
        return
    state_opts = {
        'OK': 'instance_status_ok',
        'STOPPED': 'instance_stopped',
        'TERMINATED': 'instance_terminated',
        'EXISTS': 'instance_exists',
        'RUNNING': 'instance_running',
    }
    if state not in state_opts:
        raise ValueError("Cannot wait for state {0}, invalid state".format(state))
    waiter = module.client('ec2').get_waiter(state_opts[state])
    try:
        waiter.wait(
            InstanceIds=ids,
            WaiterConfig={
                'Delay': 15,
                'MaxAttempts': module.params.get('wait_timeout', 600) // 15,
            }
        )
    except botocore.exceptions.WaiterConfigError as e:
        module.fail_json(msg="{0}. Error waiting for instances {1} to reach state {2}".format(
            to_native(e), ', '.join(ids), state))
    except botocore.exceptions.WaiterError as e:
        module.warn("Instances {0} took too long to reach state {1}. {2}".format(
            ', '.join(ids), state, to_native(e)))


def diff_instance_and_params(instance, params, ec2=None):
    """boto3 instance obj, module params"""
    if ec2 is None:
        ec2 = module.client('ec2')

    changes_to_apply = []
    id_ = instance['InstanceId']

    ParamMapper = namedtuple('ParamMapper', ['param_key', 'instance_key', 'attribute_name', 'add_value'])

    def value_wrapper(v):
        return {'Value': v}

    param_mappings = [
        ParamMapper('ebs_optimized', 'EbsOptimized', 'ebsOptimized', value_wrapper),
        ParamMapper('termination_protection', 'DisableApiTermination', 'disableApiTermination', value_wrapper),
    ]

    for mapping in param_mappings:
        if params.get(mapping.param_key) is not None:
            value = ec2.describe_instance_attribute(Attribute=mapping.attribute_name, InstanceId=id_)
            if value[mapping.instance_key]['Value'] != params.get(mapping.param_key):
                arguments = dict(
                    InstanceId=instance['InstanceId'],
                    # Attribute=mapping.attribute_name,
                )
                arguments[mapping.instance_key] = mapping.add_value(params.get(mapping.param_key))
                changes_to_apply.append(arguments)

    return changes_to_apply


def find_instances(ec2, ids=None, filters=None):
    paginator = ec2.get_paginator('describe_instances')
    if ids:
        return list(paginator.paginate(
            InstanceIds=ids,
        ).search('Reservations[].Instances[]'))
    elif filters is None:
        # TODO use default filters to find instances
        # probably subnet-id + name
        return []
        module.fail_json(msg="No filters provided when they were required")
        base_filters = {
            'subnet-id': module.params.get('name')
        }
        if module.params.get('vpc_subnet_id'):
            base_filters['subnet-id'] = module.params.get('vpc_subnet_id')
        if module.params.get('name'):
            base_filters['tag:Name'] = module.params.get('name')
        return paginator.paginate(
            Filters=ansible_dict_to_boto3_filter_list(base_filters)
        ).search('Reservations[].Instances[]')
    elif filters is not None:
        for key in filters.keys():
            if not key.startswith("tag:"):
                filters[key.replace("_", "-")] = filters.pop(key)
        return list(paginator.paginate(
            Filters=ansible_dict_to_boto3_filter_list(filters)
        ).search('Reservations[].Instances[]'))
    return []


@AWSRetry.jittered_backoff()
def get_default_vpc(ec2):
    vpcs = ec2.describe_vpcs(Filters=ansible_dict_to_boto3_filter_list({'isDefault': 'true'}))
    if len(vpcs.get('Vpcs', [])):
        return vpcs.get('Vpcs')[0]
    return None


@AWSRetry.jittered_backoff()
def get_default_subnet(ec2, vpc):
    subnets = ec2.describe_subnets(
        Filters=ansible_dict_to_boto3_filter_list({
            'vpc-id': vpc['VpcId'],
            'state': 'available',
            'default-for-az': 'true',
        })
    )
    if len(subnets.get('Subnets', [])):
        # to have a deterministic sorting order, we sort by AZ so we'll always pick the `a` subnet first
        # there can only be one default-for-az subnet per AZ, so the AZ key is always unique in this list
        by_az = sorted(subnets.get('Subnets'), key=lambda s: s['AvailabilityZone'])
        return by_az[0]
    return None


def ensure_instance_state(state, ec2=None):
    if ec2 is None:
        module.client('ec2')
    if state in ('running', 'started'):
        changed, failed, instances = change_instance_state(filters=module.params.get('filters'), desired_state='RUNNING')

        if failed:
            module.fail_json(
                msg="Unable to start instances",
                reboot_success=list(changed),
                reboot_failed=failed)

        module.exit_json(
            msg='Instances started',
            reboot_success=list(changed),
            changed=bool(len(changed)),
            reboot_failed=[],
            instances=[pretty_instance(i) for i in instances],
        )
    elif state in ('restarted', 'rebooted'):
        changed, failed, instances = change_instance_state(
            filters=module.params.get('filters'),
            desired_state='STOPPED')
        changed, failed, instances = change_instance_state(
            filters=module.params.get('filters'),
            desired_state='RUNNING')

        if failed:
            module.fail_json(
                msg="Unable to restart instances",
                reboot_success=list(changed),
                reboot_failed=failed)

        module.exit_json(
            msg='Instances restarted',
            reboot_success=list(changed),
            changed=bool(len(changed)),
            reboot_failed=[],
            instances=[pretty_instance(i) for i in instances],
        )
    elif state in ('stopped',):
        changed, failed, instances = change_instance_state(
            filters=module.params.get('filters'),
            desired_state='STOPPED')

        if failed:
            module.fail_json(
                msg="Unable to stop instances",
                stop_success=list(changed),
                stop_failed=failed)

        module.exit_json(
            msg='Instances stopped',
            stop_success=list(changed),
            changed=bool(len(changed)),
            stop_failed=[],
            instances=[pretty_instance(i) for i in instances],
        )
    elif state in ('absent', 'terminated'):
        terminated, terminate_failed, instances = change_instance_state(
            filters=module.params.get('filters'),
            desired_state='TERMINATED')

        if terminate_failed:
            module.fail_json(
                msg="Unable to terminate instances",
                terminate_success=list(terminated),
                terminate_failed=terminate_failed)
        module.exit_json(
            msg='Instances terminated',
            terminate_success=list(terminated),
            changed=bool(len(terminated)),
            terminate_failed=[],
            instances=[pretty_instance(i) for i in instances],
        )


def change_instance_state(filters, desired_state, ec2=None):
    """Takes STOPPED/RUNNING/TERMINATED"""
    if ec2 is None:
        ec2 = module.client('ec2')

    changed = set()
    to_change = set(i['InstanceId'] for i in find_instances(ec2, filters=filters))

    for id_ in to_change:
        try:
            if desired_state == 'TERMINATED':
                resp = ec2.terminate_instances(InstanceIds=[id_])
                [changed.add(i['InstanceId']) for i in resp['TerminatingInstances']]
            if desired_state == 'STOPPED':
                resp = ec2.stop_instances(InstanceIds=[id_])
                [changed.add(i['InstanceId']) for i in resp['StoppingInstances']]
            if desired_state == 'RUNNING':
                resp = ec2.start_instances(InstanceIds=[id_])
                [changed.add(i['InstanceId']) for i in resp['StartingInstances']]
        except (botocore.exceptions.ClientError, botocore.exceptions.BotoCoreError):
            # we don't care about exceptions here, as we'll fail out if any instances failed to terminate
            pass

    if changed:
        await_instances(ids=list(changed), state=desired_state)

    change_failed = list(to_change - changed)
    instances = find_instances(ec2, ids=list(to_change))
    return changed, change_failed, instances


def pretty_instance(i):
    instance = camel_dict_to_snake_dict(i, ignore_list=['Tags'])
    instance['tags'] = boto3_tag_list_to_ansible_dict(i['Tags'])
    return instance


def determine_iam_role(name_or_arn, iam):
    if re.match(r'^arn:aws:iam::\d+:instance-profile/[\w+=/,.@-]+$', name_or_arn):
        return name_or_arn
    if iam is None:
        iam = module.client('iam')
    try:
        role = iam.get_instance_profile(InstanceProfileName=name_or_arn)
        return role['InstanceProfile']['Arn']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            module.fail_json_aws(msg="Could not find instance_role {0}".format(name_or_arn))
        module.fail_json_aws(msg="An error occurred while searching for instance_role "
                                 "{0}. Please try supplying the full ARN.".format(name_or_arn))
    module.fail_json(msg="Could not retrieve instance_role {0}".format(name_or_arn))


def main():
    global module
    '''
    - ec2_instance:
        name: foobar # shortcut for `tags: {Name: foobar}`
        tags:
          foo: bar
          bas: quuux
        image: ami-1234 # or can be an object as below
        image:
          id: ami-1234
          ramdisk: ....
          kernel: ....
        instance_type: t2.micro
        network:
          ebs_optimized: yes
          assign_public_ip: yes
          private_ip_address: 10.0.0.9
          ipv6_addresses:
          - dead:beef:cafe:8080
          source_dest_check: yes
        user_data: .....
        placement_group: .....
        tower_job_template:
          # mutex w/ UserData
          template_id: ....
        network_interfaces:
          - public_ip_address: yes
            source_dest_check: yes
            ......
          # mutex w/ network
        vpc_subnet_id: subnet-123456
        security_group: id_or_name (alias for a single-item list of security_groups)
        security_groups:
        - id_or_name
        tenancy: dedicated|shared
        detailed_monitoring: yes
        termination_protection: yes
        instance_initiated_shutdown_behavior: stop|terminate
        instance_role: [arn or name]
        elastic_gpu: ....
        spot_market:
          interrupt_behavior:
          price:
          type:  onetime|persistent
          launch_group:
          wait_timeout: 300
    '''
    argument_spec = ec2_argument_spec()
    argument_spec.update(dict(
        state=dict(default='present', choices=['present', 'started', 'running', 'stopped', 'restarted', 'rebooted', 'terminated', 'absent']),
        wait=dict(default=True, type='bool'),
        wait_timeout=dict(default=600, type='int'),
        # count=dict(default=1, type='int'),
        assign_public_ip=dict(type='bool'),
        image=dict(type='dict'),
        image_id=dict(type='str'),
        instance_type=dict(default='t2.micro', type='str'),
        user_data=dict(type='str'),
        tower_callback=dict(type='dict'),
        ebs_optimized=dict(type='bool'),
        vpc_subnet_id=dict(type='str', aliases=['subnet_id']),
        security_groups=dict(default=[], type='list'),
        security_group=dict(type='str'),
        instance_role=dict(type='str'),
        name=dict(type='str'),
        tags=dict(type='dict'),
        purge_tags=dict(type='bool', default=False),
        filters=dict(type='dict', default=None),
        instance_initiated_shutdown_behavior=dict(type='str', choices=['stop', 'terminate']),
        termination_protection=dict(type='bool'),
        instance_ids=dict(default=[], type='list'),
        network=dict(default=None, type='dict'),
    ))
    # running/present are synonyms
    # as are terminated/absent
    module = AnsibleAWSModule(
        argument_spec=argument_spec,
        mutually_exclusive=[
            ['network_interfaces', 'network'],
            ['security_groups', 'security_group'],
            ['network_interfaces', 'security_group'],
            ['network_interfaces', 'security_groups'],
            ['tower_callback', 'user_data'],
            ['image_id', 'image'],
        ],
        supports_check_mode=True
    )

    def client(self, service):
        region, ec2_url, aws_connect_kwargs = get_aws_connection_info(self, boto3=True)
        return boto3_conn(self, conn_type='client', resource=service,
                          region=region, endpoint=ec2_url, **aws_connect_kwargs)

    def resource(self, service):
        region, ec2_url, aws_connect_kwargs = get_aws_connection_info(self, boto3=True)
        return boto3_conn(self, conn_type='resource', resource=service,
                          region=region, endpoint=ec2_url, **aws_connect_kwargs)

    module.client = lambda x: client(module, x)
    module.resource = resource

    state = module.params.get('state')
    ec2 = module.client('ec2')
    if module.params.get('filters') is None:
        filters = {
            # all states except shutting-down and terminated
            'instance-state-name': ['pending', 'running', 'stopping', 'stopped']
        }
        if state == 'stopped':
            # only need to change instances that aren't already stopped
            filters['instance-state-name'] = ['pending', 'running']

        if isinstance(module.params.get('instance_ids'), text_type):
            filters['instance-id'] = [module.params.get('instance_ids')]
        elif isinstance(module.params.get('instance_ids'), list) and len(module.params.get('instance_ids')):
            filters['instance-id'] = module.params.get('instance_ids')
        else:
            if not module.params.get('vpc_subnet_id'):
                sub = get_default_subnet(ec2, get_default_vpc(ec2))
                if sub is not None:
                    filters['subnet-id'] = sub['SubnetId']
            else:
                filters['subnet-id'] = [module.params.get('vpc_subnet_id')]

            if module.params.get('name'):
                filters['tag:Name'] = [module.params.get('name')]

            if module.params.get('image_id'):
                filters['image-id'] = [module.params.get('image_id')]
            elif (module.params.get('image') or {}).get('id'):
                filters['image-id'] = [module.params.get('image', {}).get('id')]

        module.params['filters'] = filters

    existing_matches = find_instances(ec2, filters=module.params.get('filters'))
    changed = False

    if state not in ('terminated', 'absent') and existing_matches:
        for match in existing_matches:
            warn_if_public_ip_assignment_changed(match)
            tags_to_set, tags_to_delete = compare_aws_tags(
                boto3_tag_list_to_ansible_dict(match['Tags']),
                module.params.get('tags') or {},
                purge_tags=module.params.get('purge_tags'),
            )
            if tags_to_set:
                ec2.create_tags(Resources=[match['InstanceId']], Tags=ansible_dict_to_boto3_tag_list(tags_to_set))
                changed |= True
            if tags_to_delete:
                delete_with_current_values = {k: match['Tags'].get(k) for k in tags_to_delete}
                ec2.delete_tags(Resources=[match['InstanceId']], Tags=ansible_dict_to_boto3_tag_list(delete_with_current_values))
                changed |= True

    if state in ('present', 'running', 'started'):
        if len(existing_matches):
            if state in ('running', 'started') and [i for i in existing_matches if i['State']['Name'] != 'running']:
                ins_changed, failed, instances = change_instance_state(filters=module.params.get('filters'), desired_state='RUNNING')
                module.exit_json(
                    changed=bool(len(ins_changed)) or changed,
                    instances=[pretty_instance(i) for i in instances],
                    instance_ids=[i['InstanceId'] for i in instances],
                )
            changes = diff_instance_and_params(existing_matches[0], module.params)
            for c in changes:
                module.warn(str(c))
                ec2.modify_instance_attribute(**c)
            altered = find_instances(ec2, ids=[i['InstanceId'] for i in existing_matches])
            module.exit_json(
                changed=bool(len(changes)) or changed,
                instances=[pretty_instance(i) for i in altered],
                instance_ids=[i['InstanceId'] for i in altered],
                changes=changes,
            )
        try:
            instance_spec = build_run_instance_spec(module.params)
            instance_response = ec2.run_instances(**instance_spec)
            instances = instance_response['Instances']
            instance_ids = [i['InstanceId'] for i in instances]

            for ins in instances:
                # some things (src_dest_check, etc) are instance-attr only and cannot be set at `RunInstance` time
                diff_instance_and_params(ins, module.params)

            await_instances(instance_ids)
            instances = ec2.get_paginator('describe_instances').paginate(
                InstanceIds=instance_ids
            ).search('Reservations[].Instances[]')

            module.exit_json(
                changed=True,
                instances=[pretty_instance(i) for i in instances],
                instance_ids=instance_ids,
                spec=instance_spec,
            )
        except (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError) as e:
            module.fail_json_aws(e, msg=instance_spec)
        module.exit_json(changed=True, spec=instance_spec)
    elif state in ('restarted', 'rebooted', 'stopped', 'absent', 'terminated'):
        if existing_matches:
            ensure_instance_state(state, ec2)
        else:
            module.exit_json(
                msg='No matching instances found',
                changed=False,
                instances=[],
            )
    else:
        module.fail_json(msg="We don't handle the state {0}".format(state))


if __name__ == '__main__':
    main()
