
import xarray as xr

file_path = "/mnt/c/DevNet/NASA/NC Files/MERRA2_200.19960117.SUB.Clifton.nc"
ds = xr.open_dataset(file_path)
print(ds)


'''
import xarray as xr
import os

# Folder containing all .nc files
nc_folder = "/mnt/c/DevNet/NASA/NC Files/"

# Get all .nc file paths
nc_files = [os.path.join(nc_folder, f) for f in os.listdir(nc_folder) if f.endswith('.nc')]

# Open and merge all .nc files along 'lon', 'lat', and 'time'
ds_merged = xr.open_mfdataset(nc_files, combine="by_coords", parallel=True)  # Remove concat_dim

# Save merged file
ds_merged.to_netcdf("Arizona_Combined_Data.nc")
'''