"""
Using boto3 to REMOVE files on the S3.

This script is utilizing the boto3 session
all AWS credentials are setup through AWS CLI with
`aws configure`

A configuration JSON file is used to specify the
local root directories, S3 bucket name, and other parameters.
The script will walk through the specified local directories,
find all netcdf files, and upload them to the specified S3 bucket.

"""

import os
import sys
import logging
import boto3
from botocore.exceptions import ClientError
from psl_upload_s3 import load_config, setup_logging

# Upload function with TransferConfig
def boto3_remove(
    obj_name: str,
    s3_bucket_name: str,
    s3_client,
):
    """using boto3 to remove files from S3

    Parameters
    ----------
    obj_name : str
        S3 object name
    s3_bucket_name : str
        S3 bucket name
    s3_client : 
        boto3 S3 client object
    """

    # check object existence
    try:
        # Check if the object exists by calling head_object
        s3_client.head_object(Bucket=s3_bucket_name, Key=obj_name)
        # Delete the object from the bucket
        s3_client.delete_object(Bucket=s3_bucket_name, Key=obj_name)
        logging.info(
            "Object %s removed from the bucket '%s'.",
            obj_name,
            s3_bucket_name
        )
    except ClientError as e:
        # If the object doesn't exist
        logging.error("Object does not exist: %s", e)

    return

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python psl_remove_s3.py <config_file.json>")
        sys.exit(1)

    config_file = sys.argv[1]

    # Load config
    dict_config = load_config(config_file)
    LOG_FILE = dict_config["log_file_name"]
    S3_CEFI = dict_config["s3_buck_name"]
    local_root_dirs = dict_config["local_root_dirs"]
    release = dict_config["release"]

    # Setup logging file
    setup_logging(LOG_FILE)

    # Create a single session and S3 client
    session = boto3.Session()
    s3_client_remove = session.client("s3")

    # portal root directories
    #  used to calculate relative path based on the local_root_dirs
    portal_root_dir = '/Projects/CEFI/regional_mom6/cefi_portal/'

    # walk through all netcdf files under the root directory
    for root_dir in local_root_dirs:
        for dirpath, dirnames, filenames in os.walk(root_dir):
            # Skip symlinked directories
            if os.path.islink(dirpath):
                continue

            # Calculate the relative path from root_dir
            relative_dirpath = os.path.relpath(dirpath, portal_root_dir)

            # Find release folder name
            release_folder = os.path.basename(os.path.normpath(dirpath))
            # Skip last folder not equal to the required release
            if release_folder != release:
                continue

            for filename in filenames:
                # only upload netcdf files
                if filename.endswith('.nc') :
                    objectname = os.path.join(relative_dirpath,filename)
                    boto3_remove(
                        obj_name = objectname,
                        s3_bucket_name = S3_CEFI,
                        s3_client = s3_client_remove,
                    )


    # Close the S3 client
    s3_client_remove.close()
    logging.info("Removal completed.")
    # Close the logging file
    logging.shutdown()
