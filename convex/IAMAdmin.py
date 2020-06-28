"""AWS IAM functions """
import boto3
import logging
import json
from botocore.exceptions import ClientError

class IAMAdmin:
    ############################################################
    # constructor
    ############################################################
    def __init__(self, region, path, profile_name):

        self.name = "IAM Admin"
        self.LOGGER = logging.getLogger(__name__)
        self.path = path
        self.region = region
        self.session = boto3.session.Session(profile_name=profile_name, region_name=region)
        self.client = boto3.client('iam')
        self.iamresource = boto3.resource('iam')

    ############################################################
    # str
    ############################################################
    def __str__(self):

        return repr("IAM Admin")

    ############################################################
    # create role
    ############################################################
    def create_role(self, role_name, role_description):
        """
        Get the security policy of a bucket.
        Usage is shown in usage_demo at the end of this module.
        :param session: The.
        :retu
        """
        try:
            iam = self.session.client('iam')
            role_name = role_name
            description = role_description # 'BOTO3 convex test role'

            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "",
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "ec2.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }

            response = iam.create_role(
                Path='/' + self.path + '/',
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description=description,
                MaxSessionDuration=3600
            )


            role_arn = response['Role']['Arn']

            print("+ Created IAM role: {}".format(role_arn))

        except ClientError as error:

            self.LOGGER.exception("Couldn't create role named '%s' in region=%s.",
                             role_name, self.region)

            raise error
        else:
            return role_arn

    ############################################################
    # create Policy and attach to Role
    ############################################################
    def create_policy(self, policy_name, role_name, bucket_name):
        """
        Get the security policy of a bucket.
        Usage is shown in usage_demo at the end of this module.
        :param policy_name: The name of the policy.
        :param role_name : The name of the Role to attach the policy to
        """
        try:
            iam = self.session.client('iam')

            managed_policy = {
              "Version": "2012-10-17",
              "Statement": [
                {
                  "Effect": "Allow",
                  "Action": [
                    "s3:ListBucket"
                  ],
                 "Resource": [
                    "arn:aws:s3:::" + bucket_name
                  ]
                },
                {
                  "Effect": "Allow",
                  "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:DeleteObject",
                    "s3:PutObjectAcl"
                  ],
                  "Resource": [
                     "arn:aws:s3:::" + bucket_name + "/*"
                  ]
                }
              ]
            }

            response = iam.create_policy(
                PolicyName=policy_name,
                PolicyDocument=json.dumps(managed_policy)
            )
            policy_arn = response['Policy']['Arn']

            iam.attach_role_policy(
                PolicyArn=policy_arn,
                RoleName= role_name
            )

            print("+ Created IAM policy: {}".format(response))

        except ClientError as error:

            self.LOGGER.exception("Couldn't create policy named '%s' in region=%s for policy %s.",
                             policy_name, self.region, role_name)

            raise error
        else:
            return response

    ############################################################
    # delete role
    ############################################################
    def delete_role(self, role_name):
        """
        Delete IAM role
        :param role_name : The name of the Role to delete
        """
        try:
            iam = self.session.client('iam')

            role = self.iamresource.Role(name=role_name)

            # Get all Managed Policies and detatch them
            print(f"Removing Managed Policies from {role.name}")
            [role.detach_policy(PolicyArn=policy.arn)
             for policy in role.attached_policies.all()]

            # Get all Instance Profiles and detatch them
            print(f"Removing role from InstanceProfiles")
            [profile.remove_role(RoleName=role.name)
             for profile in role.instance_profiles.all()]

            # Get all Inline Policies and delete them
            print(f"Deleting Inline Policies")
            [role_policy.delete() for role_policy in role.policies.all()]

            role.delete()

            print("+ Deleted IAM Role: %s", role_name)

        except ClientError as error:

            self.LOGGER.exception("Couldn't delete role named '%s' in region=%s ",
                             role_name, self.region)

            raise error
        else:
            return True


    ############################################################
    # create Instance Profile
    ############################################################
    def create_instance_profile(self, profile_name):
        """
        Create Instance Profile
        Usage is shown in usage_demo at the end of this module.
        :param session: The.
        :retu
        """
        try:
            iam = self.session.client('iam')

            instance_profile = iam.create_instance_profile(
                InstanceProfileName=profile_name,
                Path='/' + self.path + '/'
            )

            profile_arn = instance_profile['InstanceProfile']['Arn']

            print(" Created IAM Instance profile: %s ", profile_arn)

        except ClientError as error:

            self.LOGGER.exception("Couldn't create role named '%s' in region=%s.",
                             profile_name, self.region)

            #raise error
        else:
            return profile_arn


    ############################################################
    # Add Role to Instance Profile
    ############################################################
    def add_role_instance_profile(self, profile_name, profile_arn, role_name):
        """
        Add Role to instance profile
        :param session: The.
        :retu
        """
        try:
            iam = self.session.client('iam')

            instance_role_profile = iam.add_role_to_instance_profile(
                InstanceProfileName=profile_name,
                RoleName=role_name
            )

            print(" Added Role to IAM Instance profile: %s ", profile_arn)

        except ClientError as error:

            self.LOGGER.exception("Couldn't Add role named '%s to instance profile: ' in region=%s.",
                             role_name, profile_name, self.region)

            raise error
        else:
            return profile_arn


    ############################################################
    # Get Instance Profile
    ############################################################
    def get_instance_profile(self, profile_name):
        """
        Create Instance Profile
        Usage is shown in usage_demo at the end of this module.
        :param session: The.
        :retu
        """
        try:

            instance_profile = self.iamresource.InstanceProfile(profile_name)

            profile_arn = instance_profile.arn

            print(" Get IAM Instance profile: %s ", profile_arn)

        except ClientError as error:

            self.LOGGER.exception("Couldn't get instance profile named '%s' in region=%s.",
                             profile_name, self.region)

            #raise error
        else:
            return profile_arn


