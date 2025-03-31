# cefi-cloud-transfer

The first step is to transfer the netCDF data files to cloud storage. The scripts (in the /transfer subdirectory) are devided by cloud provider since there a a few details that differ regarding authentication, but largely the process is the same for both. For AWS I generated a bash script, for GCS I downloaded the files using Python, but either technique would work equally well.

There is also a directory with Python code to make the individual kerchunk index files (one per netCDF file) and a script to transfer the results. There is also code to make a multifile index of all variables and all times in the seasonal reforecast directory.

Finally, there are examples of how to read the data using the xarray library and to create a plot (to verify that you can read data and understand how it's organized).


![multi](https://github.com/user-attachments/assets/cb5124ec-1b8b-4d59-89b1-9796b7f6d95e)
