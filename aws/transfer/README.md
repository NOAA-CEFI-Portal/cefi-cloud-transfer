# AWS transfer code

N.B. you will need the AWS secret and key granted by the NODD program and will have to set those up in the ~/.cefi_env file in order to access the buket.

[make_scripts.py](make_scripts.py) - creates bash scripts that will use curl to download and pipe data from the portal TDS and onto the AWS bucket.
[transfer_json_to_s3.py](transfer_json_to_s3.py) - after creating the kerchunk index files (see [aws/kerchunk](https://github.com/NOAA-CEFI-Portal/cefi-cloud-transfer/tree/main/aws/kerchunk)) use this script to transfer the results to s3
