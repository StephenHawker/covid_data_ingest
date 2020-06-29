"""S3 functions"""
import logging
import uuid
import json
import boto3

from botocore.exceptions import ClientError


class StorageBucket:
    """
    S3 Storage Bucket functions
    """
    ############################################################
    # constructor
    ############################################################
    def __init__(self, region):
        """
        Constructor
        :param region: The region for the bucket
        """

        self.name = "Storage Bucket"
        self.region = region
        self.LOGGER = logging.getLogger(__name__)
        self.session = boto3.session.Session(profile_name='default', region_name=region)
        self.s3client = boto3.client('s3')

        self.s3resource = boto3.resource('s3')
        #self.bucket = self.s3resource.Bucket('name')

    ############################################################
    # str
    ############################################################
    def __str__(self):

        return repr("Storage Bucket")

    ############################################################
    # Create S3 bucket name (must be unique)
    ############################################################
    def create_bucket_name(self, bucket_prefix):
        """
        Generate unique bucket name
        :param bucket_prefix: Bucket prefix
        """
        # The generated bucket name must be between 3 and 63 chars long
        return ''.join([bucket_prefix, str(uuid.uuid4())])

    ############################################################
    # Create S3 bucket - region eu-west-2
    ############################################################
    def create_bucket(self, bucket_name_prefix):
        """
        Create Bucket
        :param bucket_name_prefix: Bucket prefix
        """
        try:

            self.bucketname = self.create_bucket_name(bucket_name_prefix)
            bucket_response = self.s3resource.create_bucket(Bucket=self.bucketname,
                                                            CreateBucketConfiguration={
                                                                'LocationConstraint': self.region})
            bucket_response.wait_until_exists()

            self.LOGGER.info("Created bucket '%s' in region=%s",
                             bucket_response.name,
                             self.s3resource.meta.client.meta.region_name)
            self.LOGGER.info("Bucket name %s and Region : %s ", self.bucketname, self.region)

        except ClientError as error:

            self.LOGGER.exception("Couldn't create bucket named '%s' in region=%s.",
                                  self.bucketname, self.region)
            if error.response['Error']['Code'] == 'IllegalLocationConstraintException':
                self.LOGGER.error("When the session Region is anything other than us-east-1, "
                                  "you must specify a LocationConstraint that matches the "
                                  "session Region. The current session Region is %s and the "
                                  "LocationConstraint Region is %s.",
                                  self.s3resource.meta.client.meta.region_name,
                                  self.region)
            raise error
        else:
            return bucket_response

    ############################################################
    # create temp file
    ############################################################
    def create_temp_file(self, size, file_name, file_content):
        """
        create_temp_file
        :param size: File size
        :param file_name: file_name
        :param file_name: file_content
        """
        try:
            random_file_name = ''.join([str(uuid.uuid4().hex[:6]), file_name])
            with open(random_file_name, 'w') as f:
                f.write(str(file_content) * size)
            return random_file_name

        except Exception as error:

            self.LOGGER.exception("Error in create_temp_file : %s", repr(error))
            self.LOGGER.exception("Error in create_temp_file - filename - %s  ", file_name)
            raise Exception("Error in read_file_contents - filename - %s  ", file_name)


    ############################################################
    # Retrieve the list of existing buckets
    ############################################################
    def list_buckets(self):
        """
        create_temp_file
        """
        try:
            response = self.client.list_buckets()

            # Output the bucket names
            self.LOGGER('Existing buckets:')
            for bucket in response['Buckets']:
                self.LOGGER('Name: %s', {bucket["Name"]})

        except ClientError as error:

            self.LOGGER.exception("Couldn't list buckets")
            self.LOGGER.exception("Error:  %s", repr(error))


    ############################################################
    # Check if a bucket exists
    ############################################################
    def bucket_exists(self, bucket_name):
        """
        Determine whether a bucket with the specified name exists.
        Usage is shown in usage_demo at the end of this module.
        :param bucket_name: The name of the bucket to check.
        :return: True when the bucket exists; otherwise, False.
        """
        try:
            self.s3resource.meta.client.head_bucket(Bucket=bucket_name)
            self.LOGGER.info("Bucket %s exists.", bucket_name)
            exists = True

        except ClientError as error:
            self.LOGGER.warning("Bucket %s doesn't exist or you don't have access to it.",
                                bucket_name)

            exists = False
        return exists

    ############################################################
    # Check if a bucket exists
    ############################################################
    def delete_bucket(self, bucket):
        """
        Delete a bucket. The bucket must be empty or an error is raised.
        :param bucket: The bucket to delete.
        """
        try:
            bucket = self.s3resource.Bucket(bucket)
            response = bucket.delete()
            bucket.delete()
            bucket.wait_until_not_exists()
            self.LOGGER.info("Bucket %s successfully deleted.", bucket.name)

        except ClientError as error:
            self.LOGGER.exception("Couldn't delete bucket %s.", bucket.name)
            self.LOGGER.exception("Error '%s'.", repr(error))
            raise

    ############################################################
    # Add file to bucket
    ############################################################
    def add_file_to_bucket(self, full_file_path, file_name, prefix):
        """
        Add file to bucket.
        :param file_name: The file name.
        :param prefix: The prefix for the file.
        """
        try:
            full_path_key = prefix + file_name
            self.s3resource.meta.client.upload_file(full_file_path,
                                                    self.bucketname,
                                                    full_path_key)

        except ClientError as error:
            self.LOGGER.exception("Couldn't add file %s to bucket %s.",
                                  full_path_key,
                                  self.bucketname)
            self.LOGGER.exception("Error '%s'.", repr(error))
            raise

    ############################################################
    # Get ACL of bucket
    ############################################################
    def get_acl(self, bucket_name):
        """
        Get the ACL of the specified bucket.
        Usage is shown in usage_demo at the end of this module.
        :param bucket_name: The name of the bucket to retrieve acl.
        :return: The ACL of the bucket.
        """

        try:
            acl = self.s3resource.Bucket(bucket_name).Acl()
            self.LOGGER.info("Got ACL for bucket %s owned by %s.",
                             bucket_name, acl.owner['DisplayName'])

        except ClientError as error:
            self.LOGGER.exception("Couldn't get ACL for bucket %s.", bucket_name)
            self.LOGGER.exception("Error '%s'.", repr(error))
            raise
        else:
            return acl

    ############################################################
    # put policy on bucket
    ############################################################
    def put_policy(self, bucket_name, policy):
        """
        Apply a security policy to a bucket.
        :param bucket_name: The name of the bucket to receive the policy.
        :param policy: The policy to apply to the bucket.
        """

        try:
            # The policy must be in JSON format.
            self.s3resource.Bucket(bucket_name).Policy().put(Policy=json.dumps(policy))
            self.LOGGER.info("Put policy %s for bucket '%s'.", policy, bucket_name)

        except ClientError as error:
            self.LOGGER.exception("Couldn't apply policy to bucket '%s'.", bucket_name)
            self.LOGGER.exception("Error '%s'.", repr(error))
            raise

    ############################################################
    # get policy on bucket
    ############################################################
    def get_policy(self, bucket_name):
        """
        Get the security policy of a bucket.
        :param bucket_name: The bucket to retrieve.
        :return: The security policy of the specified bucket.
        """
        try:
            policy = self.s3resource.Bucket(bucket_name).Policy()
            self.LOGGER.info("Got policy %s for bucket '%s'.", policy.policy, bucket_name)

        except ClientError as error:
            self.LOGGER.exception("Couldn't get policy for bucket '%s'.", bucket_name)
            self.LOGGER.exception("Error '%s'.", repr(error))
            raise
        else:
            return json.loads(policy.policy)

    ############################################################
    # delete policy on bucket
    ############################################################
    def delete_policy(self, bucket_name):
        """
        Delete the security policy from the specified bucket.
        :param bucket_name: The name of the bucket to update.
        """
        try:
            self.s3resource.Bucket(bucket_name).Policy().delete()
            self.LOGGER.info("Deleted policy for bucket '%s'.", bucket_name)

        except ClientError as error:
            self.LOGGER.exception("Couldn't delete policy for bucket '%s'.", bucket_name)
            self.LOGGER.exception("Error '%s'.", repr(error))
            raise
