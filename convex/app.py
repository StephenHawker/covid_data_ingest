''' Convex Technical Test - 110620 - 1442'''

import os
import sys
import logging.config
import uuid
import json
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
        parser.add_argument('--profile', help='The aws profile to use.', required=True)
        #parser.add_argument('--keep_bucket', help='Keeps the created bucket. When not '
        #                    action='store_true')

        args = parser.parse_args()
        aws_profile = args.profile
        #aws_profile = 'default'

        LOGGER.info('Started run. main:')

        uo_helper_funct = HelperFunctions()
        security_groups = []
        security_groups.append(SECURITY_GROUP_NAME)
        filename_full_path_pq = os.path.join(DIRNAME, ADDRESS_FILE_PARQUET)

        #write parquet file
        write_parquet_file()

        LOGGER.debug("Bucket name prefix: %s ", BUCKET_NAME_PREFIX)

        uo_storage_bucket = StorageBucket(REGION)
        uo_storage_bucket.create_bucket(BUCKET_NAME_PREFIX)
        uo_storage_bucket.add_file_to_bucket(filename_full_path_pq,
                                             ADDRESS_FILE_PARQUET,
                                             '') #FILE_NAME_PREFIX

        LOGGER.debug("Bucket name : %s ", uo_storage_bucket.bucketname)
        LOGGER.debug("IAM_PATH : %s ", IAM_PATH)

        uo_iam_admin = IAMAdmin(REGION, IAM_PATH, aws_profile)

        exists_role_arn = uo_iam_admin.get_role(ROLE_NAME)
        if not exists_role_arn:
            role_arn = uo_iam_admin.create_role(ROLE_NAME,
                                             ROLE_DESCRIPTION,
                                             EC2POLICY_FILE)
        else:
            role_arn = exists_role_arn

        s3policy_dict = {}
        s3policy_dict['<bucket_name>'] = uo_storage_bucket.bucketname

        s3_policy_contents = uo_helper_funct.read_and_replace(s3policy_dict,
                                                           POLICY_TEMPLATE_FOLDER + S3POLICY_FILE).replace('\n', '')
        s3_policy_contents = json.dumps(s3_policy_contents.replace(' ', ''))

        managed_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        "arn:aws:s3:::" + uo_storage_bucket.bucketname
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
                        "arn:aws:s3:::" + uo_storage_bucket.bucketname + "/*"
                    ]
                }
            ]
        }

        s3_policy_arn_exists = uo_iam_admin.get_policy_arn(S3POLICY_NAME)['Arn']
        if not s3_policy_arn_exists:
            s3_policy_arn = uo_iam_admin.create_policy(managed_policy,
                                                       S3POLICY_NAME,
                                                       ROLE_NAME
                                                       )
        else:
            s3_policy_arn = s3_policy_arn_exists
        uo_iam_admin.attach_policy_role(s3_policy_arn, ROLE_NAME)

        exists_instance_profile = uo_iam_admin.get_instance_profile(INSTANCE_PROFILE_NAME)
        if not exists_instance_profile:
            instance_profile_arn = uo_iam_admin.create_instance_profile(INSTANCE_PROFILE_NAME)
        else:
            instance_profile_arn = exists_instance_profile

        '''
        Substitution is used to insert values into a template
        file - in this case the aws config
        '''

        config_dict = {}
        config_dict['<instanceprofile>'] = INSTANCE_PROFILE_NAME
        config_dict['<role_arn>'] = role_arn

        config_contents = uo_helper_funct.read_and_replace(config_dict,
                                                           FILE_TEMPLATE_FOLDER + CONFIG_TEMPLATE)


        uo_helper_funct.write_file(config_contents, DIRNAME + '/' + CONFIG_FILENAME)

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
        uo_instance_mgmt = InstanceManagement(REGION, aws_profile)

        startup_script = UO_HELPER.read_file_contents(FILE_TEMPLATE_FOLDER + STARTUP_SCRIPT)

        instance_id = uo_instance_mgmt.create_instance(INSTANCE_KEY_FILE_NAME,
                                                     INSTANCE_KEYPAIR,
                                       role_arn,
                                       ROLE_NAME,
                                       startup_script,
                                       security_groups,
                                       IMAGE_ID
                                       )

        '''
        Associate the instance profile with the instance
        '''
        uo_instance_mgmt.associate_profile_to_instance( INSTANCE_PROFILE_NAME
                                                     , instance_profile_arn
                                                     , instance_id
                                                     )


        assumerole_policy_dict = {}
        assumerole_policy_dict['<role_arn>'] = role_arn
        assumerole_contents = uo_helper_funct.read_and_replace(assumerole_policy_dict,
                                                           POLICY_TEMPLATE_FOLDER + ASSUMEROLE_POLICY_FILE)

        assumerole_contents = json.dumps(assumerole_contents.replace('\n', '').replace(' ',''))

        assum_role_policy=        {
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Resource": role_arn
            }
        }

        uo_iam_admin.create_policy(assum_role_policy,
                                   EC2POLICY_NAME,
                                   ROLE_NAME)

        uo_instance_mgmt.get_instance_metadata(instance_id)
        ip_address = uo_instance_mgmt.ec2info[instance_id]['public_ip']

        '''
        SSH onto the instance and execute set of commands from a file
        - update and install software e.g. R
        '''
        uo_ssh = SSHInstanceAdmin(INSTANCE_KEY_FILE_NAME,
                                  ip_address
                                 )
        uo_ssh.ssh_connect(INSTANCE_USER)

        install_script_list = uo_helper_funct.read_to_list(FILE_TEMPLATE_FOLDER
                                                           + '/' + INSTALL_SCRIPT)
        uo_ssh.execute_commands(install_script_list)

        chmod_command_list = []
        cmd_list_env = []
        #Set env variable


        cmd_list_env.append('mkdir ' + AWS_CREDENTIALS_FOLDER)
        cmd_list_env.append('export AWS_ROLE_ARN=' + role_arn)

        uo_ssh.execute_commands(cmd_list_env)

        chmod_command_list.append('chmod 400 ' +
                                  AWS_CREDENTIALS_FOLDER +
                                  CONFIG_FILENAME)


        #Upload file to correct credentials location
        uo_ssh.upload_single_file(DIRNAME + '/' + CONFIG_FILENAME,
                                  AWS_CREDENTIALS_FOLDER + CONFIG_FILENAME
                                  )
        #Set permissions on AWS credentials
        uo_ssh.execute_commands(chmod_command_list)

        '''
        Get copy bucket
        '''

        copy_bucket_dict = {}
        copy_bucket_list = [] #copy_bucket.sh

        copy_bucket_dict['<bucket_name>'] = uo_storage_bucket.bucketname
        copy_bucket_dict['<file_name>'] = ADDRESS_FILE_PARQUET
        copy_bucket_dict['<location>']  = R_SCRIPT_REMOTE_LOC

        copy_bucket_contents = uo_helper_funct.read_and_replace(copy_bucket_dict,
                                                           FILE_TEMPLATE_FOLDER + COPY_BUCKET_TEMPLATE)

        copy_bucket_list = copy_bucket_contents.splitlines()
        ##Get bucket from S3 to EC2 instance
        uo_ssh.execute_commands(copy_bucket_list)


        '''
        Get R script template
        
        '''
        '''
        Substitution is used to insert values into a template
        file - in this case r script
        '''
        rscript_dict = {}

        rscript_dict['<bucket_name>'] = uo_storage_bucket.bucketname
        rscript_dict['<file_name>'] = ADDRESS_FILE_PARQUET
        rscript_dict['<location>'] = R_SCRIPT_REMOTE_LOC


        rscript_contents = uo_helper_funct.read_and_replace(rscript_dict,
                                                           FILE_TEMPLATE_FOLDER + RSCRIPT_TEMPLATE)

        uo_helper_funct.write_file(rscript_contents,
                                   R_SCRIPT)

        '''
        Copy r script to instance
        '''
        uo_ssh.upload_single_file(R_SCRIPT, R_SCRIPT_REMOTE_LOC)

        chmod_command_list_r = []
        chmod_command_list_r.append('sudo chmod +x ' +
                                  R_SCRIPT_REMOTE_LOC +
                                  R_SCRIPT)

        #Set permissions on r script
        uo_ssh.execute_commands(chmod_command_list_r)


        run_r_script = []
        run_r_script.append('sudo Rscript ' + R_SCRIPT_REMOTE_LOC +
                            R_SCRIPT)

        uo_ssh.execute_commands(run_r_script)

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
    # Write local parquet file
    ############################################################
def write_parquet_file():

    LOGGER.debug("input address file : %s ", ADDRESSES_FILE)
    LOGGER.debug("Parquet address file : %s ", ADDRESS_FILE_PARQUET)

    """Read file from disk"""
    df_addr = pandas.read_csv(ADDRESSES_FILE)
    address_table = pa.Table.from_pandas(df_addr)
    pq.write_table(address_table, ADDRESS_FILE_PARQUET)


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
        EC2POLICY_NAME = CONFIGIMPORT["convex.ec2assumerolepolicy"]
        IAM_PATH = CONFIGIMPORT["convex.iam_path"]
        ADDRESSES_FILE = CONFIGIMPORT["convex.address_file"]
        ADDRESS_FILE_PARQUET = CONFIGIMPORT["convex.address_file_parquet"]
        INSTANCE_PROFILE_NAME = CONFIGIMPORT["convex.instance_profile_name"]
        SECURITY_GROUP_NAME = CONFIGIMPORT["convex.security_group_name"]
        INSTANCE_KEY_FILE_NAME = CONFIGIMPORT["convex.instance_keyfile"]
        INSTANCE_KEYPAIR = CONFIGIMPORT["convex.instance_keypair"]
        CONFIG_FILENAME = CONFIGIMPORT["convex.config_filename"]
        IMAGE_ID = CONFIGIMPORT["convex.image_id"]

        #Template folders
        FILE_TEMPLATE_FOLDER = DIRNAME + CONFIGIMPORT["convex.file_template_folder"]
        INSTANCE_TEMPLATE_FOLDER = DIRNAME + CONFIGIMPORT["convex.instance_template_folder"]
        POLICY_TEMPLATE_FOLDER = DIRNAME + CONFIGIMPORT["convex.policy_template_folder"]
        ROLE_TEMPLATE_FOLDER = DIRNAME + CONFIGIMPORT["convex.role_template_folder"]

        CONFIG_TEMPLATE = CONFIGIMPORT["convex.config_template"]
        AWS_CREDENTIALS_FOLDER = CONFIGIMPORT["convex.aws_credentials_folder"]
        INSTALL_SCRIPT = CONFIGIMPORT["convex.install_script"]
        STARTUP_SCRIPT = CONFIGIMPORT["convex.startup_script"]

        S3POLICY_FILE = CONFIGIMPORT["convex.s3policyfile"]
        EC2POLICY_FILE = CONFIGIMPORT["convex.ec2policyfile"]
        ASSUMEROLE_POLICY_FILE = CONFIGIMPORT["convex.assumerolepolicyfile"]

        RSCRIPT_TEMPLATE = CONFIGIMPORT["convex.r_script_template"]
        COPY_BUCKET_TEMPLATE = CONFIGIMPORT["convex.copy_bucket_template"]
        R_SCRIPT = CONFIGIMPORT["convex.r_script"]
        R_SCRIPT_REMOTE_LOC = CONFIGIMPORT["convex.r_script_remote_loc"]
        INSTANCE_USER = CONFIGIMPORT["convex.instance_user"]

        main()

    except Exception:
        LOGGER.error("An Exception in : MAIN :" + __name__)
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("Convex tech test failed - please check")
