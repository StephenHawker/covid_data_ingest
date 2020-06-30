"""AWS EC2 Instance functions """

import logging
import time
import boto3

from collections import defaultdict
from botocore.exceptions import ClientError

class InstanceManagement:
    """
    ECS Instance Management
    """
    ############################################################
    # constructor
    ############################################################
    def __init__(self, region, profile_name):
        """
        Constructor
        :param region: The region to use
        :param profile_name: Profile to connect with
        """
        self.name = "Instance Management"
        self.LOGGER = logging.getLogger(__name__)
        self.region = region
        self.session = boto3.session.Session(profile_name=profile_name, region_name=region)
        self.client = boto3.client('ec2')
        self.ec2 = boto3.resource('ec2')
        self.instances = None
        self.ec2info = None

    ############################################################
    # str
    ############################################################
    def __str__(self):
        return repr("Instance Management")


    ############################################################
    # Create EC2 Instance
    ############################################################
    def create_instance(self,
                        key_file,
                        key_pair,
                        role_arn,
                        role_name,
                        startup_script,
                        security_groups,
                        image_id
                        ):
        """
        Get the security policy of a bucket.
        :param key_file: The keyfile to use to connect.
        :param key_pair: The key pair to use
        :param role_arn: The role arn to use
        :param role_name: The role name to use
        :param startup_script: Startup script for bootstrapping
        :param security_groups: security groups to use
        :retu
        """
        try:

            ec2 = self.session.client('ec2')

            key_pair_del_resp = ec2.delete_key_pair(KeyName=key_pair)
            key_pair_create_resp = ec2.create_key_pair(KeyName=key_pair)

            key_pair_out = str(key_pair_create_resp["KeyMaterial"])

            with open(key_file, 'w') as opened_file:
                opened_file.write(key_pair_out)

            self.instances = self.ec2.create_instances(
                ImageId=image_id,
                MinCount=1,
                MaxCount=1,
                InstanceType='t2.micro',
                UserData=startup_script,  # script that will bootstrap when server starts
                SecurityGroupIds=security_groups,
                KeyName=key_pair
            )

            #Wait for spin up
            time.sleep(30)
            #TODO Add waiter
            #Wait until instance is running TODO
            #instance_created_waiter = self.client.get_waiter('instance created')
            #instance_created_waiter.wait(InstanceId=[instance_id])

            for instance in self.instances:
                self.LOGGER.debug("Instance id : %s instance state : ",
                                  instance.id,
                                  instance.state)
                instance_id = instance.id
                instance_ip = instance.public_ip_address

        except ClientError as error:

            self.LOGGER.exception(self.instances)
            self.LOGGER.exception("Couldn't create EC2 Instance in region=%s.",
                                  self.region)
            self.LOGGER.exception("Error:  %s", repr(error))

            raise error
        else:
            return instance_id

    ############################################################
    # Associate Instance Profile to Instance
    ############################################################
    def associate_profile_to_instance(self,
                                      profile_name,
                                      profile_arn,
                                      instance_id):
        """
        Get the security policy of a bucket.
        :param profile_name: The profile name to associate
        :param profile_arn: The profile arn to associate
        :param instance_id: The instance_id to associate to
        :return assoc_inst_profile:
        """
        try:

            assoc_inst_profile = self.client.associate_iam_instance_profile(
                IamInstanceProfile={
                    'Arn': profile_arn,
                    'Name': profile_name
                },
                InstanceId=instance_id
            )

        except ClientError as error:
            self.LOGGER.exception("Error:  %s", repr(error))
            self.LOGGER.debug("Instance id : %s", instance_id)
            self.LOGGER.exception("Couldn't associate EC2 Instance named '%s' in region=%s.",
                                  instance_id, self.region)

            raise error
        else:
            return assoc_inst_profile


    ############################################################
    # Get Instance Metadata
    ############################################################
    def get_instance_metadata(self, instance_id):
        """
        Get the security policy of a bucket.
        :param instance_id: The instance_id to associate to
        :return assoc_inst_profile:
        """
        try:

            # Get information for all running instances
            running_instances = self.ec2.instances.filter(Filters=[{
                'Name': 'instance-state-name',
                'Values': ['running']}])

            ec2info = defaultdict()
            for instance in running_instances:
                # Add instance info to a dictionary
                ec2info[instance.id] = {
                    'type': instance.instance_type,
                    'state': instance.state['Name'],
                    'private_ip': instance.private_ip_address,
                    'public_ip': instance.public_ip_address,
                    'launch_time': instance.launch_time
                }

            self.ec2info = ec2info


        except ClientError as error:
            self.LOGGER.exception("Error:  %s", repr(error))
            self.LOGGER.debug("Instance id : %s", instance_id)
            self.LOGGER.exception("Couldn't associate EC2 Instance named '%s' in region=%s.",
                                  instance_id, self.region)

            raise error
