import xarray as xr

# Load a single NetCDF file
ds = xr.open_dataset("/mnt/c/DevNet/NASA/NC Files/MERRA2_200.19960117.SUB.Clifton.nc")

# Check dimensions and variables
print(ds)
