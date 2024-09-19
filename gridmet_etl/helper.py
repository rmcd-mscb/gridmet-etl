"""Helper Functions."""
import pandas as pd
import geopandas as gpd
from pathlib import Path
import xarray
from typing import Optional
import numpy as np

def fill_onhm_ncf(
    nfile: str,
    output_dir: str,
    feature_id: str,
    mfile: Optional[str] = "fill_missing_nearest.csv",
    genmap: Optional[bool] = False,
    var: Optional[str] = "",
    lat: Optional[str] = "",
    lon: Optional[str] = "",
) -> bool:
    """Function uses nearest-neighbor, to fill missing feature values.

    Args:
        nfile (str): NetCDF file to process.
        output_dir (str): Path to output file
        feature_id (str): Name of feature
        mfile (str): Name of new or existing mapping file
        genmap (Optional[bool], optional): Genrate mapping file. Defaults to False.
        var (Optional[str], optional): Name of variable to process for generating
            nearest neighbors
        lat ((Optional[str], optional): name of Latitude or y coordinate
        lon (Optional[bstrool], optional): Name of Longitude or x coordinate
    """
    odir = Path(output_dir)
    if not odir.exists():
        print(f"Path: {odir} does not exist")
        exit
    data = xarray.open_dataset(nfile, engine="netcdf4")  # type: ignore
    if var not in list(data.keys()):
        print(f"Error: {var} not in dataset")
        exit
    if genmap:
        # create geodatafrom from masked data of missing values
        data_1d = data[var].isel(time=[0])
        fv = data[var].encoding.get("_FillValue")
        df_mask = data_1d.where(data_1d.isnull(), drop=True)
        if df_mask.size == 0:
            print("No missing data - exiting")
            return False
        m_vals = df_mask.values[0, :]
        lon_m = df_mask[lon].values[:]
        lat_m = df_mask[lat].values[:]
        hruid_m = df_mask[feature_id].values[:]
        df_m = pd.DataFrame(
            {"featid": hruid_m, "lon": lon_m, "lat": lat_m, "tmax": m_vals}
        )
        gdf_m = gpd.GeoDataFrame(df_m, geometry=gpd.points_from_xy(df_m.lon, df_m.lat))

        # create geodatafrom from non-missing data
        df_filled = data_1d.where(data_1d.isnull() == False, drop=True)
        tmax_f_vals = df_filled.values[0, :]
        lon_f = df_filled[lon].values[:]
        lat_f = df_filled[lat].values[:]
        hruid_f = df_filled[feature_id].values
        df_f = pd.DataFrame(
            {"featid": hruid_f, "lon": lon_f, "lat": lat_f, "tmax": tmax_f_vals}
        )
        gdf_f = gpd.GeoDataFrame(df_f, geometry=gpd.points_from_xy(df_f.lon, df_f.lat))

        # use spatial-join to find nearest filled data for each missing hru-id
        nearest_m = gdf_m.sjoin_nearest(gdf_f, distance_col="distance")

        # print(nearest_m.head())
        nearest_m.drop(
            ["lon_left", "lat_left", "lon_right", "lat_right"], axis=1
        ).to_csv(odir / mfile)
    else:
        nearest_m = pd.read_csv(mfile)

    miss_index = nearest_m.featid_left.values
    fill_index = nearest_m.featid_right.values

    # fill missing values
    for dvar in data.data_vars:
        if dvar != "crs":
            print(f"processing {dvar}")
            data[dvar].loc[{feature_id: miss_index}] = (
                data[dvar].loc[{feature_id: fill_index}].values
            )

    oldfile = Path(nfile)
    # Split the filename into the base name and extension
    base_name, extension = oldfile.stem, oldfile.suffix
    new_base_name = base_name.replace("converted_", "converted_filled_")
    newfile = odir / (new_base_name + extension)
    # newfile = odir / f"{oldfile.name[:-3]}_filled.nc"

    # write new netcdf file with _filled appended to existing filename
    encoding = {}
    encoding_keys = "_FillValue"
    for data_var in data.data_vars:
        encoding[data_var] = {
            key: value
            for key, value in data[data_var].encoding.items()
            if key in encoding_keys
        }
        encoding[data_var].update(zlib=True, complevel=2)

    for data_var in data.coords:
        encoding[data_var] = {
            key: value
            for key, value in data[data_var].encoding.items()
            if key in encoding_keys
        }
        encoding[data_var].update(_FillValue=None, zlib=True, complevel=2)
    print(encoding)
    data.to_netcdf(path=newfile, encoding=encoding)
    return True

def read_elevation_values(filename):
    values = []  # List to store the elevation values
    start_collecting = False  # Flag to start collecting values
    count_values = 0  # To count the values read
    
    with open(filename, 'r') as file:
        for line in file:
            stripped_line = line.strip()  # Remove any leading/trailing whitespace
            
            if stripped_line == 'hru_elev':
                # Skip the next four lines after 'hru_elev'
                for _ in range(4):
                    next(file)
                start_collecting = True  # Set flag to start collecting after skipping
                continue
            
            if stripped_line == '####' and start_collecting:
                break  # Stop reading if we hit #### after starting to collect
            
            if start_collecting:
                try:
                    # Convert the line to a float and add to the list
                    value = float(stripped_line)
                    values.append(value)
                    count_values += 1
                except ValueError:
                    continue  # If conversion fails, skip the line
        print(f"Read {count_values} elevation values")
    return values

def pressure_at_elevation(T_avg, elevation):
    """Calculate atmospheric pressure in hPa at given elevation in meters, using average temperature in Kelvin.
    
    Args:
    T_avg (ndarray): 2D or 3D array of average temperatures in Kelvin (can be time x id or ensemble x time x id).
    elevation (ndarray): Elevation in meters matching the last dimension of T_avg.
    
    Returns:
    ndarray: Atmospheric pressure in hPa, with the same dimensions as T_avg.
    """
    P0 = 1013.25  # sea level standard atmospheric pressure in hPa
    R = 287.05  # specific gas constant for dry air, J/(kg·K)
    g = 9.80665  # acceleration due to gravity, m/s^2
    
    # Reshape elevation to be compatible with T_avg for broadcasting
    if T_avg.ndim == 3:
        elevation = elevation[np.newaxis, np.newaxis, :]  # Reshape for (ensemble, time, id)
    elif T_avg.ndim == 2:
        elevation = elevation[np.newaxis, :]  # Reshape for (time, id)

    # Compute pressure
    return P0 * np.exp(-g * elevation / (R * T_avg))

def saturation_vapor_pressure(T):
    """Calculate saturation vapor pressure in hPa for a temperature in Kelvin."""
    Tc = T - 273.15  # Convert Kelvin to Celsius
    # Magnus formula for saturation vapor pressure over water
    return 6.1094 * np.exp((17.625 * Tc) / (Tc + 243.04))

def calculate_relative_humidity(ds, elevations):
    T_avg = (ds["tmmx"].values + ds["tmmn"].values) / 2
    
    # Calculate pressure at given elevations
    pressures = pressure_at_elevation(T_avg, elevations)
    
    # Calculate saturation vapor pressure using average temperature
    e_s = saturation_vapor_pressure(T_avg)
    
    # Calculate actual vapor pressure
    e = (ds["sph"].values * pressures) / 0.622
    
    # Calculate relative humidity
    rh = (e / e_s) * 100
    
    # Add relative humidity to the dataset
    dims = ds.dims.keys()
    ds["humidity"] = xarray.DataArray(rh, dims=dims, coords={dim: ds[dim] for dim in dims})
    ds["humidity"].attrs = {
        'units': '%',
        'long_name': 'Relative Humidity',
        'description': 'Calculated from specific humidity, average temperature, and elevation using modified pressure estimation'
    }
    return ds