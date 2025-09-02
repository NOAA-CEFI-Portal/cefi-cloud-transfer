"""
Using boto3 to transfer files from the local filesystem to S3.

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
import json
import logging
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError

# load the configuration JSON file
def load_config(json_file):
    """Load directory configuration from a JSON file."""
    with open(json_file, 'r', encoding='utf-8') as jsonfile:
        return json.load(jsonfile)

# setup logging
def setup_logging(logfile_name):
    """Set up logging to write messages to a log file."""
    logging.basicConfig(
        filename=logfile_name,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

# Upload function with TransferConfig
def boto3_upload(
    local_root: str,
    file_rel_path: str,
    s3_bucket_name: str,
    upload_config,
    s3_client,
):
    """using boto3 to upload files to S3
    utilizing TransferConfig to configure multipart uploads

    Parameters
    ----------
    local_root : str
        local data root directory
    file_rel_path : str
        relative path with respect to local_root including filename
    s3_bucket_name : str
        S3 bucket name
    upload_config : _type_
        TransferConfig object to configure multipart uploads
    s3_client : _type_
        boto3 S3 client object
    """
    obj_name = file_rel_path
    local_file = os.path.join(local_root, file_rel_path)

    # check object existence
    try:
        # Check if the object exists by calling head_object
        s3_client.head_object(Bucket=s3_bucket_name, Key=obj_name)
        logging.info(
            "Object %s already exists in the bucket '%s'. Skipping upload.",
            obj_name,
            s3_bucket_name
        )
        return
    except ClientError as e:
        # If the object doesn't exist
        if e.response['Error']['Code'] == '404':
            pass
        else:
            # Handle other errors (e.g., permission issues)
            logging.error("Error checking object existence: %s", e)
            return

    # upload object
    try:
        s3_client.upload_file(
            local_file,
            s3_bucket_name,
            obj_name,
            Config=upload_config
        )
        logging.info('Uploaded: %s to %s called %s',local_file,s3_bucket_name,obj_name)
    except Exception as e:
        logging.error("Error uploading %s: %s",obj_name,e)

    return

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Usage: python psl_upload_s3.py <config_file.json>")
        sys.exit(1)

    config_file = sys.argv[1]

    # Load config
    dict_config = load_config(config_file)
    LOG_FILE = dict_config["log_file_name"]
    S3_CEFI = dict_config["s3_buck_name"]
    local_root_dirs = dict_config["local_root_dirs"]
    release = dict_config["release"]

    # Setup logging file
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    setup_logging(LOG_FILE)

    # Create a single session and S3 client
    session = boto3.Session()
    s3_client_upload = session.client("s3")

    # Configure multipart uploads (Adjust chunk size and concurrency)
    transfer_config = TransferConfig(
        multipart_threshold=100 * 1024 * 1024,  # 100MB threshold for multipart
        multipart_chunksize=50 * 1024 * 1024,   # 50MB chunk size
        max_concurrency=10,                     # Number of parallel threads
        use_threads=True                        # Enable threading
    )

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
                    boto3_upload(
                        local_root = portal_root_dir,
                        file_rel_path = objectname,
                        s3_bucket_name = S3_CEFI,
                        upload_config = transfer_config,
                        s3_client = s3_client_upload,
                    )


    # Close the S3 client
    s3_client_upload.close()
    logging.info("Upload completed.")
    # Close the logging file
    logging.shutdown()
