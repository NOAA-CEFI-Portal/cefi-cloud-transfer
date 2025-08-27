"""
Using boto3 to transfer files from the local filesystem to S3.

This script is utilizing the boto3 session
all AWS credentials are setup through AWS CLI with
`aws configure`

You will need to enter
- AWS Access Key ID
- AWS Secret Access Key
- Default region (e.g., `us-east-1`)
- Output format (leave as `None` or type `json`)

A configuration JSON file is used to specify the
local root directories, S3 bucket name, and other parameters.
The script will walk through the entire CEFI data root directory,
find all netcdf files, and upload the latest release to the specified S3 bucket.

"""

import os
import json
import logging
from datetime import datetime
import fsspec
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
import xarray as xr
from kerchunk.hdf import SingleHdf5ToZarr

# set up bucket
S3_BUCKET_NAME = 'noaa-oar-cefi-regional-mom6-pds'

# set up log file location
script_dir = os.path.dirname(os.path.abspath(__file__))
date_str = datetime.now().strftime("%Y%m%d")
LOG_FILE = 's3_upload.log'
log_path = os.path.join(script_dir, LOG_FILE)


# CEFI data root abs path
#  used to calculate relative path based on the local_root_dirs
PORTAL_DATA_PATH = '/Projects/CEFI/regional_mom6/cefi_portal/'

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
    local_file: str,
    obj_name: str,
    s3_bucket_name: str,
    upload_config,
    s3_client,
):
    """using boto3 to upload files to S3
    utilizing TransferConfig to configure multipart uploads

    Parameters
    ----------
    local_file : str
        local data absolution path including filename
    obj_name : str
        object name for the cloud storage (relative path with respect to local_root including filename)
    s3_bucket_name : str
        S3 bucket name
    upload_config : _type_
        TransferConfig object to configure multipart uploads
    s3_client : _type_
        boto3 S3 client object
    """

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

    # check local netcdf file data integrity (json file is not checked)
    if local_file.endswith('.nc'):
        try :
            ds = xr.open_dataset(local_file)
            ds.close()
        except Exception as e:
            logging.error("Error verifying local file %s: %s", local_file, e)
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

def create_file_dict(local_root_dirs):
    """Create a dictionary of files to upload.

    Parameters
    ----------
    local_root_dirs : list
        List of local root directories to search for files.

    Returns
    -------
    dict
        Dictionary that provide all files to upload and the related
        local path and cloud obj name.
        The keys are parent directories, and the values are release folders
        containing lists of dictionaries with 'local' and 'cloud' keys.
        Each dictionary contains the local file path and the cloud object name.
        ex:
        dict_files = {
            parent_dir: {
                release_folder: [
                    {'local': local_file_path, 'cloud': cloud_object_name},
                    ...
                ]
            },
            ...
        }
    """
    # walk through all netcdf files under the root directory
    dict_files = {}

    for dirpath, dirnames, filenames in os.walk(local_root_dirs):

        # Get list to see if the walk hit any netcdf files
        nc_files = [f for f in filenames if f.endswith(".nc")]

        # Skip intermediate directories that do not contain netcdf files
        if not nc_files:
            continue

        # Skip symlinked directories
        if os.path.islink(dirpath):
            continue

        # Find release folder name
        release_folder = os.path.basename(os.path.normpath(dirpath))

        # find parent directory
        parent_dir = os.path.dirname(dirpath)

        # Determine the latest release folder
        # release_date = int(release_folder[1:])

        # dict_files[parent_dir] = {release_folder: []}
        for filename in filenames:
            # only upload netcdf files
            if filename.endswith('.nc') :
                # Calculate the relative path from root_dir
                relative_dirpath = os.path.relpath(dirpath, PORTAL_DATA_PATH)

                dict_file_info = {}
                dict_file_info['local'] = os.path.join(dirpath, filename)
                dict_file_info['cloud'] = os.path.join(relative_dirpath, filename)
                (
                dict_files
                 .setdefault(parent_dir, {})
                 .setdefault(release_folder, [])
                 .append(dict_file_info)
                )

    return dict_files

def keep_latest_release(dict_files):
    """Keep only the latest release folder in the dictionary.

    Parameters
    ----------
    dict_files : dict
        Dictionary of files to upload.

    Returns
    -------
    dict
        Dictionary with only the latest release folder.
    """
    dict_outdated_releases = dict_files.copy()
    dict_latest_release = {}
    for parent_dir, dict_releases_folders in dict_files.items():
        releases = []
        for releases_folder, list_files in dict_releases_folders.items():
            release_date = int(releases_folder[1:])
            releases.append(release_date)

        # Keep the latest release folder
        latest_release = f'r{max(releases)}'

        dict_latest_release[parent_dir] = {latest_release: dict_files[parent_dir][latest_release]}

        dict_outdated_releases[parent_dir].pop(latest_release, None)

    return dict_latest_release,dict_outdated_releases

def verify_s3_file_access(s3_bucket_name: str, obj_name: str) -> bool:
    """
    Verify that the uploaded S3 file can be accessed remotely and opened with xarray.
    
    Parameters
    ----------
    s3_bucket_name : str
        S3 bucket name
    obj_name : str
        S3 object key/name
        
    Returns
    -------
    bool
        True if file can be accessed and opened with xarray, False otherwise
    """
    s3_storage_options = {
        "remote_options": {"anon": True},
        "remote_protocol": "s3",
        # "target_options": {"anon": True},
        "target_protocol": "s3",
    }
    try:
        # Construct S3 path
        s3_path = f's3://{s3_bucket_name}/{obj_name}'

        with xr.open_dataset(
            s3_path,
            engine="netcdf4",
            chunks='auto',
            storage_options=s3_storage_options
        ) as ds:
            # Basic validation - check if dataset has variables
            if len(ds.data_vars) > 0:
                logging.info("File validation passed - %s", ds[ds.attrs['cefi_variable']])
                return True
            else:
                logging.warning("File opened but contains no data variables")
                return False
                
    except Exception as e:
        logging.error("Failed to verify S3 file access: %s", e)
        return False

def gen_kerchunk_index(
    s3_path : str,
    save_dir : str,
    server : str = 's3'
)-> str:
    """
    Use Kerchunk's `SingleHdf5ToZarr` method to create a 
    `Kerchunk` index from a NetCDF file in the cloud

    Parameter
    ---------
    s3_path : str
        The S3 path to a single NetCDF file in the format of 
        f's3://{s3_bucket_name}/{obj_name}'
    save_dir : str
        The directory to save the Kerchunk index file
    server : str
        The cloud storage server to use (default: 's3')
    """
    # start a filesystem reference for publically accessible cloud storage
    fs_read = fsspec.filesystem(server, anon=True)
    s3_file_paths = fs_read.glob(s3_path)

    # file number check (need to be one file)
    if len(s3_file_paths) == 1:
        s3_file = s3_file_paths[0]
    else:
        raise ValueError("More than one file found")

    # create index file name for the cloud storage netcdf file
    filename = s3_file.split("/")[-1].strip(".nc")
    json_file = os.path.join(save_dir, f"{filename}.json")

    # check if the json file already exist locally
    if os.path.exists(json_file):
        logging.info(f"JSON file already exists, skip kerchunking: {json_file}")
        return json_file

    # open file for remote read and indexing
    with fs_read.open(s3_file, **dict(mode="rb")) as infile:
        logging_run = f"Running kerchunk index generation for {s3_file}..."
        logging.info(logging_run)

        # Chunks smaller than `inline_threshold` will be stored directly
        # in the reference file as data (as opposed to a URL and byte range).
        h5chunks = SingleHdf5ToZarr(infile, s3_file, inline_threshold=300)
        
        with open(json_file, "wb") as f:
            f.write(json.dumps(h5chunks.translate()).encode())

        return json_file

if __name__ == '__main__':
    # setup if performing kerchunking alongside file upload
    KERCHUNK_FLAG = True

    # Setup logging file
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    setup_logging(LOG_FILE)

    if KERCHUNK_FLAG:
        logging.info("Kerchunking is enabled.")
    else:
        logging.info("Kerchunking is disabled.")

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
    all_local_json_paths = []
    for parent_dir, dict_releases_folders in dict_latest.items():
        for release_folder, list_files in dict_releases_folders.items():
            for file_info in list_files:

                # Get the local file path and cloud object name for netcdf
                local_file_path = file_info['local']
                cloud_object_name = file_info['cloud']

                # Upload the netcdf file to S3
                boto3_upload(
                    local_file=local_file_path,
                    obj_name=cloud_object_name,
                    s3_bucket_name=S3_BUCKET_NAME,
                    upload_config=transfer_config,
                    s3_client=s3_client_upload
                )

                if KERCHUNK_FLAG :
                    # create kerchunk json file
                    s3_ncfile_path = f's3://{S3_BUCKET_NAME}/{cloud_object_name}'
                    local_json_path = gen_kerchunk_index(
                        s3_path=s3_ncfile_path,
                        save_dir='/home/chsu/cefi-cloud-transfer/operation/s3_kerchunk_json'
                    )
                    all_local_json_paths.append(local_json_path)

                    # Upload the json file to S3
                    s3_json_filename = cloud_object_name.split("/")[-1].strip(".nc")
                    s3_json_path = "/".join(cloud_object_name.split("/")[:-1])
                    boto3_upload(
                        local_file=local_json_path,
                        obj_name=f'{s3_json_path}/{s3_json_filename}.json',
                        s3_bucket_name=S3_BUCKET_NAME,
                        upload_config=transfer_config,
                        s3_client=s3_client_upload
                    )

    # clear the local json file
    if KERCHUNK_FLAG :
        for json_file in all_local_json_paths:
            os.remove(json_file)
        logging.info("All kerchunk index files cleared.")

    # Close the S3 client
    s3_client_upload.close()
    logging.info("Upload completed.")
    # Close the logging file
    logging.shutdown()
