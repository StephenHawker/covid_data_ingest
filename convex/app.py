''' Convex Technical Test - 110620 - 1442'''

import os
import sys
import logging.config
import uuid
import pandas
import pyarrow as pa
import pyarrow.parquet as pq

from StorageBucket import StorageBucket
from IAMAdmin import IAMAdmin
from InstanceManagement import InstanceManagement

from HelperFunctions import HelperFunctions
from SSHInstanceAdmin import SSHInstanceAdmin

"""Project Classes"""
""" Other Imports"""

############################################################
# Main
############################################################
def main():
    """Main process

    """
    try:

        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('profile', help='The aws profile to use.')
        parser.add_argument('bucket_name', help='The name of the bucket to create.')
        #parser.add_argument('region', help='The region in which to create your bucket.')
        #parser.add_argument('--keep_bucket', help='Keeps the created bucket. When not '
        #                                          'specified, the bucket is deleted '
        #                                          'at the end of the demo.',
        #                    action='store_true')

        #args = parser.parse_args()

        #create_and_delete_my_bucket(args.bucket_name, args.region, args.keep_bucket)

        LOGGER.info('Started run. main:')

        security_groups = []
        security_groups.append(SECURITY_GROUP_NAME)


        LOGGER.debug("input address file : %s ", ADDRESSES_FILE)
        LOGGER.debug("Parquet address file : %s ", ADDRESS_FILE_PARQUET)

        """Read file from disk"""
        df_addr = pandas.read_csv(ADDRESSES_FILE)
        address_table = pa.Table.from_pandas(df_addr)
        pq.write_table(address_table, ADDRESS_FILE_PARQUET)

        filename_full_path_pq = os.path.join(DIRNAME, ADDRESS_FILE_PARQUET)

        LOGGER.debug("Bucket name prefix: %s ", BUCKET_NAME_PREFIX)

        uo_storage_bucket = StorageBucket(REGION)
        uo_storage_bucket.create_bucket(BUCKET_NAME_PREFIX)
        uo_storage_bucket.add_file_to_bucket(filename_full_path_pq,
                                             ADDRESS_FILE_PARQUET,
                                             FILE_NAME_PREFIX)

        LOGGER.debug("Bucket name : %s ", uo_storage_bucket.bucketname)
        LOGGER.debug("IAM_PATH : %s ", IAM_PATH)

        uo_iam_admin = IAMAdmin(REGION, IAM_PATH, 'default')
        #role_arn = uo_iam_admin.create_role(ROLE_NAME, ROLE_DESCRIPTION)
        role_arn = 'rolearnvalue'
        #uo_iam_admin.create_policy(S3POLICY_NAME, ROLE_NAME, uo_storage_bucket.bucketname)

        startup_script = ''
        exists_instance_profile = uo_iam_admin.get_instance_profile(INSTANCE_PROFILE_NAME)
        if not exists_instance_profile:
            instance_profile_arn = uo_iam_admin.create_instance_profile(INSTANCE_PROFILE_NAME)
        else:
            instance_profile_arn = exists_instance_profile

        '''
        Substitution is used to insert values into a template
        file - in this case the aws config
        '''
        uo_helper_funct = HelperFunctions()
        config_dict = {}
        config_dict['<instanceprofile>'] = INSTANCE_PROFILE_NAME
        config_dict['<role_arn>'] = role_arn

        config_contents = uo_helper_funct.read_and_replace(config_dict,
                                                           FILE_TEMPLATE_FOLDER + CONFIG_TEMPLATE)


        uo_helper_funct.write_file(config_contents, DIRNAME + '/' + CONFIG_FILENAME)


        #FILE_TEMPLATE_FOLDER =
        #INSTANCE_TEMPLATE_FOLDER =
        #POLICY__TEMPLATE_FOLDER =
        #ROLE_TEMPLATE_FOLDER =
        #AWS_CREDENTIALS_FOLDER

        '''
        Add created role the instance profile
        '''
        #uo_iam_admin.add_role_instance_profile(INSTANCE_PROFILE_NAME,
        #                                     instance_profile_arn,
        #                                     ROLE_NAME
        #                                    )

        '''
        Instance management - create instance and get it's
        instance id
        '''
        uo_instance_mgmt = InstanceManagement(REGION, 'default')

        startup_script = UO_HELPER.read_file_contents(FILE_TEMPLATE_FOLDER + STARTUP_SCRIPT)

        #instance_id = uoInstanceMgmt.create_instance(INSTANCE_KEY_FILE_NAME,
        #                                             INSTANCE_KEYPAIR,
        #                               'arn:aws:iam::707712313852:role/convex/convex_test_role',
        #                               ROLE_NAME,
        #                               startup_script,
        #                               security_groups
        #                               )
        #instance_id = 'i-08b5b05427392fc57'

        '''
        Associate the instance profile with the instance
        '''
        #uoInstanceMgmt.associate_profile_to_instance( INSTANCE_PROFILE_NAME
        #                                             , instance_profile_arn
        #                                             , instance_id
        #                                             )


        '''
        SSH onto the instance and execute set of commands from a file
        - update and install software e.g. R
        '''
        uo_ssh = SSHInstanceAdmin(INSTANCE_KEY_FILE_NAME,
                                  '35.178.76.128'
                                 )
        uo_ssh.ssh_connect("ec2-user")
        install_script_list = uo_helper_funct.read_to_list(FILE_TEMPLATE_FOLDER
                                                           + '/' + INSTALL_SCRIPT)
        uo_ssh.execute_commands(install_script_list)


    except Exception as error:

        LOGGER.error("An Exception occurred convex tech test ")
        LOGGER.error(repr(error))
        raise Exception("Convex Tech test failed!")

    finally:

        '''
        Clean up after run
        '''
        #uo_storage_bucket.delete_bucket(uo_storage_bucket.bucketname)
        #uo_iam_admin.delete_role(ROLE_NAME)

        LOGGER.info('Completed run.')

############################################################
# Run
############################################################

if __name__ == "__main__":

    try:

        # create LOGGER
        LOGGER = logging.getLogger('convex')
        DIRNAME = os.path.dirname(__file__)
        FILENAME_INI = os.path.join(DIRNAME, 'convex.ini')
        UO_HELPER = HelperFunctions()

        CONFIGIMPORT = UO_HELPER.load_config(FILENAME_INI)

        LOG_PATH = CONFIGIMPORT["logging.log_path"]
        LOG_FILE = CONFIGIMPORT["logging.log_file"]
        THE_LOG = LOG_PATH + "\\" + LOG_FILE
        LOGGING_LEVEL = CONFIGIMPORT["logging.logginglevel"]

        LEVELS = {'debug': logging.DEBUG,
                  'info': logging.INFO,
                  'warning': logging.WARNING,
                  'error': logging.ERROR,
                  'critical': logging.CRITICAL}

        LEVEL = LEVELS.get(LOGGING_LEVEL, logging.DEBUG)
        logging.basicConfig(level=LEVEL)

        HANDLER = logging.handlers.RotatingFileHandler(THE_LOG, maxBytes=1036288, backupCount=5)
        # create FORMATTER
        FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        HANDLER.setFormatter(FORMATTER)
        LOGGER.addHandler(HANDLER)

        BUCKET_NAME_PREFIX = CONFIGIMPORT["convex.bucketnameprefix"]
        REGION = CONFIGIMPORT["convex.region"]
        FILE_NAME_PREFIX = CONFIGIMPORT["convex.filenameprefix"]
        ROLE_NAME = CONFIGIMPORT["convex.rolename"]
        ROLE_DESCRIPTION = CONFIGIMPORT["convex.rolenamedescription"]
        S3POLICY_NAME = CONFIGIMPORT["convex.s3policyname"]
        IAM_PATH = CONFIGIMPORT["convex.iam_path"]
        ADDRESSES_FILE = CONFIGIMPORT["convex.address_file"]
        ADDRESS_FILE_PARQUET = CONFIGIMPORT["convex.address_file_parquet"]
        INSTANCE_PROFILE_NAME = CONFIGIMPORT["convex.instance_profile_name"]
        SECURITY_GROUP_NAME = CONFIGIMPORT["convex.security_group_name"]
        INSTANCE_KEY_FILE_NAME = CONFIGIMPORT["convex.instance_keyfile"]
        INSTANCE_KEYPAIR = CONFIGIMPORT["convex.instance_keypair"]
        CONFIG_FILENAME = CONFIGIMPORT["convex.config_filename"]

        #Template folders
        FILE_TEMPLATE_FOLDER = DIRNAME + CONFIGIMPORT["convex.file_template_folder"]
        INSTANCE_TEMPLATE_FOLDER = DIRNAME + CONFIGIMPORT["convex.instance_template_folder"]
        POLICY__TEMPLATE_FOLDER = DIRNAME + CONFIGIMPORT["convex.policy_template_folder"]
        ROLE_TEMPLATE_FOLDER = DIRNAME + CONFIGIMPORT["convex.role_template_folder"]

        CONFIG_TEMPLATE = CONFIGIMPORT["convex.config_template"]
        AWS_CREDENTIALS_FOLDER = CONFIGIMPORT["convex.aws_credentials_folder"]
        INSTALL_SCRIPT = CONFIGIMPORT["convex.install_script"]
        STARTUP_SCRIPT = CONFIGIMPORT["convex.startup_script"]

        main()

    except Exception:
        LOGGER.error("An Exception in : MAIN :" + __name__)
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("Convex tech test failed - please check")
