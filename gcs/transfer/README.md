# Upload data files from the portal TDS to Google Cloud Storage

You need a service account which has permission to write to the bucket. The key will be associate with a project. The key file location and project ID are set in the .cefi_env file. 

[cp_to_google.ipynb](cp_to_google.ipynb) - Read the TDS catalog and copy files to Google Cloud Storage
[cp_json_to_google.ipynb](cp_json_to_google.ipynb) - Once the kerchunk files have been crated transfer them to Google Cloud Storage.
