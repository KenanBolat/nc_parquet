import xarray as xr
import numpy as np
import os
import glob
import datetime

# script run time date measurements
start_time = datetime.datetime.now()

# script settings 
file_name = r"data.nc"
process_path = r"."
subset = True

# read the netcdf data into xarray dataset

ds_main = xr.open_dataset(os.path.join(process_path, file_name))
if subset:
    # subset
    country_bounding_boxes = {
        'TR': ('Turkey', (26.0433512713, 35.8215347357, 44.7939896991, 42.1414848903)),
    }

    min_lon = country_bounding_boxes['TR'][1][0]
    min_lat = country_bounding_boxes['TR'][1][1]
    max_lon = country_bounding_boxes['TR'][1][2]
    max_lat = country_bounding_boxes['TR'][1][3]

    mask_lon = (ds_main.lon >= min_lon) & (ds_main.lon <= max_lon)
    mask_lat = (ds_main.lat >= min_lat) & (ds_main.lat <= max_lat)
    ds = ds_main.where(mask_lon & mask_lat, drop=True)
    # Date filter

    ds = ds.sel(time1=slice("2022-05-01", "2022-05-02"))
    del ds_main
else:
    ds = ds_main
df = ds.to_dataframe()
df.to_parquet('df.parquet.gzip',
              compression='gzip')

end_time = datetime.datetime.now()
print(f"Total duration:{end_time - start_time}")
