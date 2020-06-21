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

from HelperFunctions import HelperFunctions
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

        parser = argparse.ArgumentParser()
        parser.add_argument('bucket_name', help='The name of the bucket to create.')
        parser.add_argument('region', help='The region in which to create your bucket.')
        parser.add_argument('--keep_bucket', help='Keeps the created bucket. When not '
                                                  'specified, the bucket is deleted '
                                                  'at the end of the demo.',
                            action='store_true')

        args = parser.parse_args()

        #create_and_delete_my_bucket(args.bucket_name, args.region, args.keep_bucket)

        LOGGER.info('Started run. main:')
        addresses_file = configImport["convex.address_file"]
        address_file_parquet = configImport["convex.address_file_parquet"]

        LOGGER.debug("input address file : %s ", addresses_file)

        """Read file from disk"""
        df_addr = pandas.read_csv(CSV_FILE)
        address_table = pa.Table.from_pandas(df_addr)
        pq.write_table(address_table, address_file_parquet)

        uoStorageBucket = StorageBucket()

        put_policy_desc = {
            'Version': '2012-10-17',
            'Id': str(uuid.uuid1()),
            'Statement': [{
                'Effect': 'Allow',
                'Principal': {'AWS': 'arn:aws:iam::111122223333:user/convex'},
                'Action': [
                    's3:GetObject',
                    's3:PutObject',
                    's3:ListBucket'
                ],
                'Resource': [
                    f'arn:aws:s3:::{bucket.name}/*',
                    f'arn:aws:s3:::{bucket.name}'
                ]
            }]
        }


        LOGGER.info('Completed run.')

    except Exception:
        LOGGER.error("An Exception occurred convex ACS Ingest ")
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("!")


############################################################
# Run
############################################################

if __name__ == "__main__":

    try:

        dirname = os.path.dirname(__file__)
        filename_ini = os.path.join(dirname, 'convex.ini')
        UO_HELPER = HelperFunctions()

        configImport = UO_HELPER.load_config(filename_ini)

        LOG_PATH = configImport["logging.log_path"]
        LOG_FILE = configImport["logging.log_file"]
        THE_LOG = LOG_PATH + "\\" + LOG_FILE
        LOGGING_LEVEL = configImport["logging.logginglevel"]
        CSV_FILE = configImport["convex.address_file"]

        LEVELS = {'debug': logging.DEBUG,
                  'info': logging.INFO,
                  'warning': logging.WARNING,
                  'error': logging.ERROR,
                  'critical': logging.CRITICAL}

        # create LOGGER
        LOGGER = logging.getLogger('convex')
        LEVEL = LEVELS.get(LOGGING_LEVEL, logging.NOTSET)
        logging.basicConfig(level=LEVEL)

        HANDLER = logging.handlers.RotatingFileHandler(THE_LOG, maxBytes=1036288, backupCount=5)
        # create FORMATTER
        FORMATTER = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        HANDLER.setFormatter(FORMATTER)
        LOGGER.addHandler(HANDLER)

        main()

    except Exception:
        LOGGER.error("An Exception in : MAIN :" + __name__)
        LOGGER.error(str(sys.exc_info()[0]))
        LOGGER.error(str(sys.exc_info()[1]))
        # print getattr(e, 'message', repr(e))
        # print(e.message)
        raise Exception("Fire station ingest failed - please check")
