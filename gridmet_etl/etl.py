"""Main module."""
import os
import geopandas as gpd
import pint_xarray
import pandas as pd
import sys
import xarray as xr
from gridmet_etl.helper import fill_onhm_ncf
from pathlib import Path
from gdptools import AggGen, ClimRCatData
import importlib.resources as pkg_resources
from gdptools import UserCatData
import time
from distributed import Client, get_client
import numpy as np

def shutdown_existing_cluster():
    try:
        # Attempt to get a reference to an existing cluster
        client = get_client()
        print(f"Found an existing Dask cluster: {client}")
        print("Shutting down the existing Dask cluster.")
        
        # Shut down the cluster
        client.shutdown()
        del client
        print("Existing Dask cluster has been shut down.")
    except ValueError:
        # If get_client() raises a ValueError, no existing client was found
        print("No existing Dask cluster found.")



os.environ["USE_PYGEOS"] = "0"

class CFSv2ETL:
    """Class for fetching gridmet version of cfsv2 and interpolating to netcdf."""

    def __init__(self):
        self.cfsv2_vars = {
            "ws": "vs",
            "tmax": "tmmx",
            "tmin": "tmmn",
            "ppt": "pr",
            "sph": "sph"
        }
        self.partial = False
        self.fill_missing = False
        self.vars = ["tmmx", "tmmn", "pr"]

    def initialize(
        self,
        target_file: str,
        optpath: str,
        weights_file: str,
        feature_id: str,
        method: int = [1],
        fileprefix: str="",
        partial: bool = False,
        fillmissing: bool = False,
    ):
        """
        Initialize the CFSv2ETL class:
            1) initialize geopandas dataframe of concatenated hru_shapefiles
            2) initialize climate data using xarray
        :param target_file: Input path, downloaded gridmet files will be saved here.  Must also contain shapefile used to
                        generate the weights file.
        :param optpath: Path to write newly generated netcdf file containing values for vars for each HRU
        :param weights_file: Weights file, based on geometry file: target_file, that was used to generate weights file
        :param start_date: if extraction type date then start date in 'YYYY-MM-DD"
        :param end_date: if extraction type date then end date in 'YYYY-MM-DD"
        :param fileprefix: String to add to both downloaded gridment data and mapped hru file

        :return: success or failure
        """
        self.proc_median_files = []
        self.partial = partial
        self.fillmissing = fillmissing
        self.method = method
        self.target_file = target_file
        self.feature_id = feature_id
        self.fileprefix = fileprefix
        if Path(self.target_file).exists():
            print(f"input path exists {self.target_file}", flush=True)
        else:
            sys.exit(f"Input Path does not exist: {self.target_file} - EXITING")

        self.optpath = Path(optpath)
        if self.optpath.exists():
            print("output path exists", flush=True)
        else:
            sys.exit(f"Output Path does not exist: {self.optpath} - EXITING")

        self.wghts_file = Path(weights_file)
        if self.wghts_file.exists():
            print("weights file exists", self.wghts_file, flush=True)
        else:
            sys.exit(f"Weights file does not exist: {self.wghts_file} - EXITING")

        print(Path.cwd())

        # # glob.glob produces different results on Win and Linux. Adding sorted makes result consistent
        # # filenames = sorted(glob.glob('*.shp'))
        # # use pathlib glob
        # # glob is here because original nhm had multiple shapefiles
        # filenames = sorted(self.iptpath.glob("*.shp"))
        # self.gdf = pd.concat(
        #     [geopandas.read_file(f) for f in filenames], sort=True
        # ).pipe(geopandas.GeoDataFrame)
        # self.gdf.reset_index(drop=True, inplace=True)
        self.read_target_based_on_suffix()
        print(f"The source file: {self.target_file}", flush=True)
        print(f"the shapefile header is: {self.gdf.head()}", flush=True)

        # Download netcdf subsetted data
        data_id = "cfsv2_gridmet"
        try:
           with pkg_resources.open_binary('gridmet_etl.data', 'cfsv2.json') as file:
                cat = pd.read_json(file)
        except Exception as e:
            print(f"Error loading CFSV2 catalog {e}")
            return False
        # Create a dictionary of parameter dataframes for each variable
        cat_params = [
            cat.query(f'id == "{data_id}" & variable == "{_var}"').to_dict(
                orient="records"
            )[0]
            for _var in self.vars
        ]
        cat_dict = dict(zip(self.vars, cat_params))
        self.cat_dict = cat_dict

        # get start and end dates.
        tmp_dict = self.cat_dict.get(self.vars[0])
        tmp_ds = xr.open_dataset(tmp_dict.get("URL"))
        time_np = tmp_ds['time'].values
        # Extract the first and last date
        first_date = time_np[0]
        last_date = time_np[-1]

        # Convert numpy datetime64 objects to string format '%Y-%m-%d'
        first_date_str = str(first_date)[:10]  # Slicing to get 'YYYY-MM-DD'
        last_date_str = str(last_date)[:10]    # Slicing to get 'YYYY-MM-DD'

        print(f"First date: {first_date_str}")
        print(f"Last date: {last_date_str}")
        self.start_date = first_date_str
        self.end_date = last_date_str

        return True
    
    def run_weights(self):
        print("starting dask client")
        shutdown_existing_cluster()
        if self.method == 2:
            ds_list = []
            for key in self.cat_dict:
                print(f"processing {key}")
                # client = Client()
                # print(client.dashboard_link)
                cat = self.cat_dict.get(key)
                x_name = cat.get("X_name")
                y_name = cat.get("Y_name")
                t_name = cat.get("T_name")
                proj_ds = cat.get("crs")

                stt = time.perf_counter()
                dst = xr.open_dataset(cat.get("URL"), chunks={t_name: 'auto', y_name: 'auto', x_name: 'auto', 'ens': 'auto'})
                # ds = xr.open_dataset(cat.get("URL"), chunks={})
                ens_list = []
                for n in dst.ens.values:
                    new_ds = dst.sel(ens=n)
                    new_ds = new_ds.transpose("time", "lat", "lon")
                    # print(new_ds.head())
                    user_data = UserCatData(
                        ds=new_ds,
                        proj_ds=proj_ds,
                        x_coord=x_name,
                        y_coord=y_name,
                        t_coord=t_name,
                        var=[key],
                        f_feature=self.gdf,
                        proj_feature=self.gdf.crs,
                        id_feature=self.feature_id,
                        period=[self.start_date, self.end_date]
                    )

                    agg_gen = AggGen(
                        user_data=user_data,
                        stat_method="mean",
                        agg_engine="serial",
                        agg_writer="netcdf",
                        weights=str(self.wghts_file),
                        out_path=str(self.optpath),
                        file_prefix=self.fileprefix + key + "_" + str(int(n)),
                    )
                    self.proc_median_files.append(f"{self.optpath}/{self.fileprefix}{key}_{int(n)}.nc")

                    _ngdf, _ds_out = agg_gen.calculate_agg()
                    ens_list.append(f"{self.optpath}/{self.fileprefix}{key}_{int(n)}.nc")
                # Concatenate along a new dimension called 'ensemble'
                datasets = [xr.open_dataset(file) for file in ens_list]
                combined_ds = xr.concat(datasets, dim='ens', combine_attrs='override')

                combined_ds = combined_ds.assign_coords(ens=np.arange(len(ens_list)))

                ds_list.append(combined_ds)

            
            concat_ds = xr.merge(ds_list)
            concat_ds.to_netcdf(self.optpath / (self.fileprefix + ".nc"))

        if self.method == 1:
            ds_list = []
            for key in self.cat_dict:
                print(f"processing {key}")
                client = Client()
                print(client.dashboard_link)
                cat = self.cat_dict.get(key)
                x_name = cat.get("X_name")
                y_name = cat.get("Y_name")
                t_name = cat.get("T_name")
                proj_ds = cat.get("crs")

                stt = time.perf_counter()
                dst = xr.open_dataset(cat.get("URL"), chunks={t_name: 'auto', y_name: 'auto', x_name: 'auto', 'ens': 'auto'})
                # ds = xr.open_dataset(cat.get("URL"), chunks={})
                median = dst[key].median(dim='ens')
                median.attrs = dst[key].attrs
                median_computed = median.compute()
                ds = median_computed.transpose("time", "lat", "lon")
                ent = time.perf_counter()
                print(f"Median calculated in {ent-stt} seconds")
                client.close()
                del client

                user_data = UserCatData(
                    ds=ds.to_dataset(),
                    proj_ds=proj_ds,
                    x_coord=x_name,
                    y_coord=y_name,
                    t_coord=t_name,
                    var=[key],
                    f_feature=self.gdf,
                    proj_feature=self.gdf.crs,
                    id_feature=self.feature_id,
                    period=[self.start_date, self.end_date]
                )

                agg_gen = AggGen(
                    user_data=user_data,
                    stat_method="mean",
                    agg_engine="serial",
                    agg_writer="netcdf",
                    weights=str(self.wghts_file),
                    out_path=str(self.optpath),
                    file_prefix=self.fileprefix + key,
                )
                f_path = self.optpath / (self.fileprefix + key + ".nc")
                _ngdf, _ds_out = agg_gen.calculate_agg()
                # print(_ds_out)
                ds_list.append(f_path)
            concat_ds = xr.open_mfdataset(ds_list)
            concat_ds.to_netcdf(self.optpath / (self.fileprefix + ".nc"))

        print(f"Processed files: {self.proc_median_files}")
        return True
    
    def finalize(self) -> None:
        f_path = self.optpath / (self.fileprefix + ".nc")
        ds = xr.open_dataset(f_path)
        var_rename = {
            "tmmx": "tmax",
            "tmmn": "tmin",
            "pr": "prcp"
        }
        if self.method == 2:
            for n in ds.ens.values:
                dst = ds.sel(ens=n)
                dst = dst.rename_vars(var_rename)
                dst = dst.rename_dims({self.feature_id: "nhru"})
                dst = dst.rename({self.feature_id: "nhru"})
                # ds = ds.rename(dims_rename)
                dst = dst.pint.quantify({"lat": None, "lon": None})
                dst = dst.pint.to({"tmax": "degC", "tmin": "degC"})
                dst = dst.pint.dequantify()
                # Converted file name
                conv_f = self.optpath / f"{self.start_date}_converted_{int(n)}.nc"
                dst.to_netcdf(conv_f, format="NETCDF4", engine="netcdf4")
                print(f"converted file written to {conv_f}")
                ensemble_path = self.optpath / "ensembles"
                if not ensemble_path.exists():
                    ensemble_path.mkdir()
                # Fill missing data using nearest neighbor approach.
                if self.fillmissing:
                    print("filling missing values")
                    response = fill_onhm_ncf(
                        nfile=conv_f,
                        output_dir=ensemble_path,
                        mfile="filltest.csv",
                        var="tmax",
                        lat="lat",
                        lon="lon",
                        feature_id="nhru",
                        genmap=True,
                    )
                    if not response:
                        conv_f.rename(ensemble_path / f"{self.start_date}_filled_converted_{int(n)}.nc")

                    print(f"finished filling missing values for ensemble:{n}")
            print("Cleaning intermediate files.")
            # Iterate over each item in the directory
            for item in self.optpath.iterdir():
                # Check if the item is a file and ends with '.nc'
                if item.is_file() and item.name.endswith('.nc'):
                    # Remove the file
                    try:
                        item.unlink()
                        # print(f"Removed: {item}")
                    except Exception as e:
                        print(f"Error removing {item}: {e}")


        elif self.method == 1:
            ds = ds.rename_vars(var_rename)
            # ds = ds.swap_dims(dims_rename)
            ds = ds.rename_dims({self.feature_id: "nhru"})
            ds = ds.rename({self.feature_id: "nhru"})
            # ds = ds.rename(dims_rename)
            ds = ds.pint.quantify({"lat": None, "lon": None})
            ds = ds.pint.to({"tmax": "degC", "tmin": "degC"})
            ds = ds.pint.dequantify()
            # Converted file name
            conv_f = self.optpath / f"{self.start_date}_converted_.nc"
            ds.to_netcdf(conv_f, format="NETCDF4", engine="netcdf4")
            print(f"converted file written to {conv_f}")
            median_path = self.optpath / "ensemble_median"
            if not median_path.exists():
                median_path.mkdir()
            # Fill missing data using nearest neighbor approach.
            if self.fillmissing:
                print("filling missing values")
                response = fill_onhm_ncf(
                    nfile=conv_f,
                    output_dir=median_path,
                    mfile="filltest.csv",
                    var="tmax",
                    lat="lat",
                    lon="lon",
                    feature_id="nhru",
                    genmap=True,
                )
                if not response:
                    conv_f.rename(median_path / f"{self.end_date}_filled_converted.nc")

            print("finished filling missing values")
            print("Cleaning intermediate files.")
            # Iterate over each item in the directory
            for item in self.optpath.iterdir():
                # Check if the item is a file and ends with '.nc'
                if item.is_file() and item.name.endswith('.nc'):
                    # Remove the file
                    try:
                        item.unlink()
                        # print(f"Removed: {item}")
                    except Exception as e:
                        print(f"Error removing {item}: {e}")

    def read_target_based_on_suffix(self):
        """
            Reads a file based on its suffix (extension).
            If the file has a '.parquet' extension, it reads it as a Parquet file.
            If the file has a '.shp' extension, it reads it as a shapefile.
        """
        file_suffix = self.target_file.lower().split('.')[-1]

        if file_suffix == 'parquet':
            # Read the file as a Parquet file
            self.gdf = gpd.read_parquet(self.target_file) #, engine='auto')
        elif file_suffix == 'shp':
            # Read the file as a shapefile
            self.gdf = gpd.read_file(self.target_file)
        else:
            print(f"Unsupported file format: {file_suffix}. Please provide a Parquet or shapefile.")

class FpoNHM:
    """Class for fetching climate data and parsing into netcdf."""

    def __init__(self, climsource="GridMetSS"):
        """
        Initialize class

        :param climsource: Constant for now but may have multiple
            choice for data sources in the future.  Currently default is
            GridMet:  http://www.climatologylab.org/gridmet.html
        """
        self.climsource = climsource
        if climsource == "GridMetSS":
            self.gmss_vars = {
                "tmax": "daily_maximum_temperature",
                "tmin": "daily_minimum_temperature",
                "ppt": "precipitation_amount",
                "rhmax": "daily_maximum_relative_humidity",
                "rhmin": "daily_minimum_relative_humidity",
                "ws": "daily_mean_wind_speed",
                "srad": "daily_mean_shortwave_radiation_at_surface",
            }
        self.partial = False
        self.fillmissing = False
        self.vars = ["tmmx", "tmmn", "pr", "rmax", "rmin", "vs"]

    def initialize(
        self,
        partial,
        target_file,
        optpath,
        weights_file,
        feature_id,
        start_date=None,
        end_date=None,
        fileprefix="",
        fillmissing="",
    ):
        """
        Initialize the fp_ohm class:
            1) initialize geopandas dataframe of concatenated hru_shapefiles
            2) initialize climate data using xarray
        :param target_file: Input path, downloaded gridmet files will be saved here.  Must also contain shapefile used to
                        generate the weights file.
        :param optpath: Path to write newly generated netcdf file containing values for vars for each HRU
        :param weights_file: Weights file, based on shapefile in target_file that was used to generate weights file
        :param start_date: if extraction type date then start date in 'YYYY-MM-DD"
        :param end_date: if extraction type date then end date in 'YYYY-MM-DD"
        :param fileprefix: String to add to both downloaded gridment data and mapped hru file

        :return: success or failure
        """
        self.feature_id = feature_id
        self.partial = partial
        self.fillmissing = fillmissing
        self.target_file = target_file
        if Path(self.target_file).exists():
            print(f"input path exists {self.target_file}", flush=True)
        else:
            sys.exit(f"Input Path does not exist: {self.target_file} - EXITING")

        self.optpath = Path(optpath)
        if self.optpath.exists():
            print("output path exists", flush=True)
        else:
            sys.exit(f"Output Path does not exist: {self.optpath} - EXITING")

        self.wghts_file = Path(weights_file)
        if self.wghts_file.exists():
            print("weights file exists", self.wghts_file, flush=True)
        else:
            sys.exit(f"Weights file does not exist: {self.wghts_file} - EXITING")

        self.start_date = start_date
        self.end_date = end_date
        print(self.end_date, type(self.end_date))
        self.fileprefix = fileprefix

        print(Path.cwd())

        print(
            f"start_date: {self.start_date} and end_date: {self.end_date}",
            flush=True,
        )
        # # glob.glob produces different results on Win and Linux. Adding sorted makes result consistent
        # # filenames = sorted(glob.glob('*.shp'))
        # # use pathlib glob
        # # glob is here because original nhm had multiple shapefiles
        # filenames = sorted(self.target_file.glob("*.shp"))
        # self.gdf = pd.concat(
        #     [geopandas.read_file(f) for f in filenames], sort=True
        # ).pipe(geopandas.GeoDataFrame)
        # self.gdf.reset_index(drop=True, inplace=True)

        self.read_target_based_on_suffix()
        print(f"The source file: {self.target_file}", flush=True)
        print(f"the shapefile header is: {self.gdf.head()}", flush=True)

        # Download netcdf subsetted data
        data_id = "gridmet"
        try:
           with pkg_resources.open_binary('gridmet_etl.data', 'catalog.parquet') as file:
                cat = pd.read_parquet(file)
        except Exception as e:
            print(f"Error loading ClimateR-Catalog: {e}")
            return False


        # Create a dictionary of parameter dataframes for each variable
        cat_params = [
            cat.query(f'id == "{data_id}" & variable == "{_var}"').to_dict(
                orient="records"
            )[0]
            for _var in self.vars
        ]
        cat_dict = dict(zip(self.vars, cat_params))

        self.user_data = ClimRCatData(
            cat_dict=cat_dict,
            f_feature=self.gdf,
            id_feature=feature_id,
            period=[self.start_date, self.end_date],
        )
        print("data initialized")
        return True

    def run_weights(self):
        if self.partial:
            print("Using Masked Mean")
            agg_gen = AggGen(
                user_data=self.user_data,
                stat_method="masked_mean",
                agg_engine="serial",
                agg_writer="netcdf",
                weights=str(self.wghts_file),
                out_path=str(self.optpath),
                file_prefix=self.fileprefix,
            )
        else:
            print("Using Mean")
            agg_gen = AggGen(
                user_data=self.user_data,
                stat_method="mean",
                agg_engine="serial",
                agg_writer="netcdf",
                weights=str(self.wghts_file),
                out_path=str(self.optpath),
                file_prefix=self.fileprefix,
            )

        self.ngdf, self.ds_out = agg_gen.calculate_agg()
        tmp = 0

    def finalize(self):
        print(Path.cwd(), flush=True)
        f_path = self.optpath / (self.fileprefix + ".nc")
        ds = xr.open_dataset(f_path)
        var_rename = {
            "daily_maximum_temperature": "tmax",
            "daily_minimum_temperature": "tmin",
            "daily_maximum_relative_humidity": "rhmax",
            "daily_minimum_relative_humidity": "rhmin",
            "precipitation_amount": "prcp",
            "daily_mean_wind_speed": "ws",
        }
        # Rename variables and convert units for NHM model input.
        ds = ds.rename_vars(var_rename)
        ds = ds.rename_dims({self.feature_id: "nhru"})
        ds = ds.rename({self.feature_id: "nhru"})
        ds = ds.pint.quantify({"lat": None, "lon": None})
        ds = ds.pint.to({"tmax": "degC", "tmin": "degC"})
        ds = ds.pint.dequantify()
        ds = ds.assign(humidity=(ds["rhmin"] + ds["rhmax"] / 2.0))
        h_attrs = {
            "coordinates": "time lat lon",
            "grid_mapping": "crs",
            "long_name": "Daily mean relative humidity",
            "units": "percent",
        }
        ds["humidity"].attrs = h_attrs
        # Converted file name
        conv_f = self.optpath / f"{self.start_date}_converted.nc"
        ds.to_netcdf(conv_f, format="NETCDF4", engine="netcdf4")
        print(f"converted file written to {conv_f}")

        # Fill missing data using nearest neighbor approach.
        if self.fillmissing:
            print("filling missing values")
            response = fill_onhm_ncf(
                nfile=conv_f,
                output_dir=self.optpath,
                mfile="filltest.csv",
                var="tmax",
                lat="lat",
                lon="lon",
                feature_id="nhru",
                genmap=True,
            )
            if not response:
                conv_f.rename(self.optpath / f"{self.start_date}_converted_filled.nc")

            print("finished filling missing values")
            print("Cleaning intermediate files.")
            # Iterate over each item in the directory
            for item in self.optpath.iterdir():
                # Check if the item is a file and ends with '.nc'
                if item.is_file() and item.name.endswith('.nc') and "filled" not in item.name:
                    # Remove the file
                    try:
                        item.unlink()
                        # print(f"Removed: {item}")
                    except Exception as e:
                        print(f"Error removing {item}: {e}")
    
    def read_target_based_on_suffix(self):
        """
            Reads a file based on its suffix (extension).
            If the file has a '.parquet' extension, it reads it as a Parquet file.
            If the file has a '.shp' extension, it reads it as a shapefile.
        """
        file_suffix = self.target_file.lower().split('.')[-1]

        if file_suffix == 'parquet':
            # Read the file as a Parquet file
            self.gdf = gpd.read_parquet(self.target_file)  #, engine='auto')
        elif file_suffix == 'shp':
            # Read the file as a shapefile
            self.gdf = gpd.read_file(self.target_file)
        else:
            print(f"Unsupported file format: {file_suffix}. Please provide a Parquet or shapefile.")
