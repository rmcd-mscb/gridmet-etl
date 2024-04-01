"""Helper Functions."""
import pandas as pd
import geopandas as gpd
from pathlib import Path
import xarray
from typing import Optional


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
    newfile = odir / f"{oldfile.name[:-3]}_filled.nc"

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
