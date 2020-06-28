''' Convex Technical Test - 110620 - 1442'''

import os
import sys
import logging.config
import simplejson as json
import pandas
import pyarrow as pa
import pyarrow.parquet as pq
import uuid
from APILookup import APILookup
from StorageBucket import StorageBucket
from IAMAdmin import IAMAdmin
from InstanceManagement import InstanceManagement

from HelperFunctions import HelperFunctions
from SSHInstanceAdmin import SSHInstanceAdmin
from collections import defaultdict

"""Project Classes"""
""" Other Imports"""


class DatabaseLoadError(Exception):
    """Database Exception

    Keyword arguments:
    Exception -- Exception
    """
    def __init__(self, data):
        self.data = data

    def __str__(self):
        return repr(self.data)

############################################################
# Main
############################################################
def main():
    """Main process

    """
    try:

        import argparse

        #parser = argparse.ArgumentParser()
        #parser.add_argument('bucket_name', help='The name of the bucket to create.')
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
        '''
        uoStorageBucket = StorageBucket(REGION)
        uoStorageBucket.create_bucket(BUCKET_NAME_PREFIX)
        uoStorageBucket.add_file_to_bucket(filename_full_path_pq, ADDRESS_FILE_PARQUET, FILE_NAME_PREFIX)
        '''
        #LOGGER.debug("Bucket name : %s ", uoStorageBucket.bucketname)
        LOGGER.debug("IAM_PATH : %s ", IAM_PATH)

        uoIAMAdmin = IAMAdmin(REGION, IAM_PATH, 'default')
        #role_arn = uoIAMAdmin.create_role(ROLE_NAME, ROLE_DESCRIPTION)
        role_arn='rolearnvalue'
        #uoIAMAdmin.create_policy(S3POLICY_NAME, ROLE_NAME, uoStorageBucket.bucketname)

        startup_script = ''
        exists_instance_profile = uoIAMAdmin.get_instance_profile(INSTANCE_PROFILE_NAME)
        if not exists_instance_profile:
            instance_profile_arn = uoIAMAdmin.create_instance_profile(INSTANCE_PROFILE_NAME)
        else:
            instance_profile_arn = exists_instance_profile

        uoHelperfunct = HelperFunctions()
        config_dict = {}
        config_dict['<instanceprofile>'] = INSTANCE_PROFILE_NAME
        config_dict['<role_arn>'] = role_arn

        config_contents = uoHelperfunct.read_and_replace(config_dict,
                                                         FILE_TEMPLATE_FOLDER + CONFIG_TEMPLATE)


        uoHelperfunct.write_file(config_contents, DIRNAME + '/' + CONFIG_FILENAME)


        #FILE_TEMPLATE_FOLDER =
        #INSTANCE_TEMPLATE_FOLDER =
        #POLICY__TEMPLATE_FOLDER =
        #ROLE_TEMPLATE_FOLDER =
        #AWS_CREDENTIALS_FOLDER

        #uoIAMAdmin.add_role_instance_profile(INSTANCE_PROFILE_NAME,
        #                                     instance_profile_arn,
        #                                     ROLE_NAME
        #                                    )

        uoInstanceMgmt = InstanceManagement(REGION, 'default')

        #instance_id = uoInstanceMgmt.create_instance(INSTANCE_KEY_FILE_NAME,
        #                                             INSTANCE_KEYPAIR,
        #                               'arn:aws:iam::707712313852:role/convex/convex_test_role',
        #                               ROLE_NAME,
        #                               startup_script,
        #                               security_groups
        #                               )
        #instance_id = 'i-08b5b05427392fc57'

        #uoInstanceMgmt.associate_profile_to_instance( INSTANCE_PROFILE_NAME
        #                                             , instance_profile_arn
        #                                             , instance_id
        #                                             )


        uoSSH = SSHInstanceAdmin(INSTANCE_KEY_FILE_NAME,
                                 '35.178.76.128'
                                 )
        uoSSH.ssh_connect()
        install_script_list = uoHelperfunct.read_to_list(FILE_TEMPLATE_FOLDER
                                                         + '/' + INSTALL_SCRIPT)
        uoSSH.execute_commands(install_script_list)


    except Exception as error:
        LOGGER.error("An Exception occurred convex tech test ")
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("!")

    finally:

        '''
        Clean up after run
        '''
        #uoStorageBucket.delete_bucket(uoStorageBucket.bucketname)
        #uoIAMAdmin.delete_role(ROLE_NAME)

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

        configImport = UO_HELPER.load_config(FILENAME_INI)

        LOG_PATH = configImport["logging.log_path"]
        LOG_FILE = configImport["logging.log_file"]
        THE_LOG = LOG_PATH + "\\" + LOG_FILE
        LOGGING_LEVEL = configImport["logging.logginglevel"]

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

        BUCKET_NAME_PREFIX = configImport["convex.bucketnameprefix"]
        REGION = configImport["convex.region"]
        FILE_NAME_PREFIX = configImport["convex.filenameprefix"]
        ROLE_NAME = configImport["convex.rolename"]
        ROLE_DESCRIPTION = configImport["convex.rolenamedescription"]
        S3POLICY_NAME = configImport["convex.s3policyname"]
        IAM_PATH = configImport["convex.iam_path"]
        ADDRESSES_FILE = configImport["convex.address_file"]
        ADDRESS_FILE_PARQUET = configImport["convex.address_file_parquet"]
        INSTANCE_PROFILE_NAME = configImport["convex.instance_profile_name"]
        SECURITY_GROUP_NAME = configImport["convex.security_group_name"]
        INSTANCE_KEY_FILE_NAME = configImport["convex.instance_keyfile"]
        INSTANCE_KEYPAIR = configImport["convex.instance_keypair"]
        CONFIG_FILENAME = configImport["convex.config_filename"]

        #Template folders
        FILE_TEMPLATE_FOLDER = DIRNAME + configImport["convex.file_template_folder"]
        INSTANCE_TEMPLATE_FOLDER = DIRNAME + configImport["convex.instance_template_folder"]
        POLICY__TEMPLATE_FOLDER = DIRNAME + configImport["convex.policy_template_folder"]
        ROLE_TEMPLATE_FOLDER = DIRNAME + configImport["convex.role_template_folder"]

        CONFIG_TEMPLATE = configImport["convex.config_template"]
        AWS_CREDENTIALS_FOLDER = configImport["convex.aws_credentials_folder"]
        INSTALL_SCRIPT = configImport["convex.install_script"]


        main()

    except Exception:
        LOGGER.error("An Exception in : MAIN :" + __name__)
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("Convex tech test failed - please check")
