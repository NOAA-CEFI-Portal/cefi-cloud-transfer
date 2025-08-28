"""
Using boto3 to transfer files from the local filesystem to S3.

This script is utilizing the boto3 session
all AWS credentials are setup through AWS CLI with
`aws configure`

A configuration JSON file is used to specify the
local root directories, S3 bucket name, and other parameters.
The script will walk through al CEFI data root directory,
find all netcdf files, and remove the outdated release on S3 bucket.

"""

import os
import logging
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from s3_upload import setup_logging, create_file_dict, keep_latest_release

# set up bucket
S3_BUCKET_NAME = 'noaa-oar-cefi-regional-mom6-pds'

# set up log file location
script_dir = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = f's3_remove.log'
log_path = os.path.join(script_dir, LOG_FILE)


# CEFI data root abs path
#  used to calculate relative path based on the local_root_dirs
PORTAL_DATA_PATH = '/Projects/CEFI/regional_mom6/cefi_portal/'

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

    # find all netcdf files under the root directory
    dict_all_files = create_file_dict(PORTAL_DATA_PATH)

    # keep only the latest release
    dict_latest, dict_outdated = keep_latest_release(dict_all_files)

    # upload files in the latest release one by one
    for parent_dir, dict_releases_folders in dict_outdated.items():
        for release_folder, list_files in dict_releases_folders.items():
            for file_info in list_files:
                local_file = file_info['local']
                cloud_object_name = file_info['cloud']
                # Upload the file to S3
                boto3_remove(
                    obj_name=cloud_object_name,
                    s3_bucket_name=S3_BUCKET_NAME,
                    s3_client=s3_client_upload
                )

    # Close the S3 client
    s3_client_upload.close()
    logging.info("Remove completed.")
    # Close the logging file
    logging.shutdown()
