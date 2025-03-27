# Scripts to make the kerchunk index files.

For our cloud storage, we create a [kurchunk](https://guide.cloudnativegeo.org/kerchunk/intro.html) index (.json) file for each netCDF (.nc) file. This allows direct access to the data using the Python xarray package with good performance.

Further more, for the seasonal reforecast, we create an aggregation of all of the variables over all of the initialization times in the reforecast. Therefore, you can access any variable at any initialization time, member, lead time by opening on virtual data set, and using the xarry select (.sel() function) to get to data of interest. [Here is an example reading the combined index file.](https://github.com/NOAA-CEFI-Portal/cefi-cloud-transfer/blob/main/aws/read/read_nwa_google_monthly_all-aws.ipynb)

1. [mk_s3_kerchunk.ipynb](mk_s3_kerchunk.ipynb) - Python script to create the kerchunk index.
2. [make_combo-aws.ipynb](make_combo-aws.ipynb) - reads all data variable files and creates a combined index. N.B. The included files are filtered by file name so the code needs updating when more variables are added.
