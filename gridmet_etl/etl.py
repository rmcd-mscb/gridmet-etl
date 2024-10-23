"""Main module."""

import os
from typing import List, Union
import geopandas as gpd
import pint_xarray
import pandas as pd
import sys
import xarray as xr
from gridmet_etl.helper import fill_onhm_ncf
from gridmet_etl.helper import read_elevation_values
from gridmet_etl.helper import calculate_relative_humidity
from pathlib import Path
from gdptools import AggGen, ClimRCatData
import importlib.resources as pkg_resources
from gdptools import UserCatData
import time
from distributed import Client, get_client
import numpy as np


def ensure_directory(path):
    """
    Ensure that a directory exists at the given path.

    Args:
        path: The path to the directory.

    Returns:
        Path: The Path object of the directory.

    Raises:
        PermissionError: If permission is denied to create the directory.
    """
    try:
        path = Path(path)  # Ensure `path` is a Path object
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            print(f"Creating new path: {path}", flush=True)
        print(f"{path} already exists", flush=True)
        return path
    except PermissionError as e:
        print(
            f"Error {e}, Permission denied: Unable to create directory at {path}",
            flush=True,
        )


def shutdown_existing_cluster():
    """
    Shut down an existing Dask cluster if one is found.

    Raises:
        ValueError: If no existing Dask cluster is found.
    """
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


def check_path(path: str, msg: str) -> Path:
    """
    Check if a given path exists.

    Args:
        path (str): The path to check.
        msg (str): Message to display.

    Returns:
        Path: The Path object if it exists.

    Raises:
        SystemExit: If the path does not exist.
    """
    path_obj = Path(path)
    if path_obj.exists():
        print(f"{msg}:{path} exists")
        return path_obj
    else:
        sys.exit(f"{msg} does not exist: {path} - EXITING")


class CFSv2ETL:
    """Class for fetching gridmet version of cfsv2 and interpolating to netcdf.

    file location: <http://thredds.northwestknowledge.net:8080/thredds/catalog/
        NWCSC_INTEGRATED_SCENARIOS_ALL_CLIMATE/cfsv2_metdata_90day/catalog.html>
    """

    def __init__(self):
        self.cfsv2_vars = {
            "ws": "vs",
            "tmax": "tmmx",
            "tmin": "tmmn",
            "ppt": "pr",
            "sph": "sph",
        }
        self.partial = False
        self.fill_missing = False
        self.vars = ["tmmx", "tmmn", "pr", "sph"]

    def initialize(
        self,
        target_file: Path,
        optpath: Path,
        weights_file: Path,
        elevation_file: Path,
        feature_id: str,
        method: int = None,
        fileprefix: str = "",
        partial: bool = False,
        fillmissing: bool = False,
    ):
        """
        Initialize the ETL process with the provided parameters.

        Args:
            target_file (Path): The path to the target file or the target geodataframe as .shp or .parquet.
            optpath (Path): The output path root.
            weights_file (Path): The path to the weights file.
            elevation_file(Path): PRMS model param file that contains hru elevations.
            feature_id (str): The feature ID.
            method (int, optional): The method to use. Defaults to None.
            fileprefix (str, optional): The file prefix. Defaults to "".
            partial (bool, optional): Whether to enable partial processing. Defaults to False.
            fillmissing (bool, optional): Whether to fill missing values. Defaults to False.

        Returns:
            bool: True if initialization is successful, False otherwise.
        """
        if method is None:
            method = 1
        self.proc_median_files = []
        self.partial = partial
        self.fillmissing = fillmissing
        self.method = method
        self.target_file = target_file
        self.elev_file = elevation_file
        self.elev = np.array(read_elevation_values(self.elev_file))
        self.feature_id = feature_id
        self.fileprefix = fileprefix
        self.optpath = optpath
        self.wghts_file = weights_file

        print(Path.cwd())

        self.read_target_based_on_suffix()
        print(f"The target file: {self.target_file}", flush=True)
        print(f"the shapefile header is: {self.gdf.head()}", flush=True)

        # Download netcdf subsetted data
        data_id = "cfsv2_gridmet"
        try:
            with pkg_resources.open_binary("gridmet_etl.data", "cfsv2.json") as file:
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
        time_np = tmp_ds["time"].values
        # Extract the first and last date
        first_date = time_np[0]
        last_date = time_np[-1]

        # Convert numpy datetime64 objects to string format '%Y-%m-%d'
        first_date_str = str(first_date)[:10]  # Slicing to get 'YYYY-MM-DD'
        last_date_str = str(last_date)[:10]  # Slicing to get 'YYYY-MM-DD'

        print(f"First date: {first_date_str}")
        print(f"Last date: {last_date_str}")
        self.start_date = first_date_str
        self.end_date = last_date_str

        # Create working directories
        self.ensemble_path = ensure_directory(
            self.optpath / "ensembles" / self.start_date
        )
        print(
            f"Ensemble path is {self.ensemble_path}: exists: {self.ensemble_path.exists()}"
        )
        self.median_path = ensure_directory(
            self.optpath / "ensemble_median" / self.start_date
        )
        print(f"Median path is {self.median_path}: exists: {self.median_path.exists()}")

        return True

    def run_weights(self) -> bool:
        """
        Run the weighted aggregation process for each key in the catalog dictionary.

        Args:
            self: The instance of the class.

        Returns:
            bool: True if the processing is successful, False otherwise.
        """
        print("Starting processing")
        shutdown_existing_cluster()
        self.initialize_client_if_needed()

        ds_list = [self.process_key(key) for key in self.cat_dict]

        concat_ds = (
            xr.merge(ds_list) if self.method == 2 else xr.open_mfdataset(ds_list)
        )
        concat_ds = calculate_relative_humidity(ds=concat_ds, elevations=self.elev)
        concat_file = self.optpath / f"{self.fileprefix}.nc"
        concat_ds.to_netcdf(concat_file)

        print(f"Processed files: {self.proc_median_files}")
        return True

    def initialize_client_if_needed(self):
        """
        Initialize a Dask client if the method is 1.
        """
        if self.method == 1:
            client = Client()
            print(client.dashboard_link)

    def process_key(self, key: str) -> Union[xr.Dataset, Path]:
        """
        Process the specified key based on the method chosen.

        Args:
            key (str): The key to process.

        Returns:
            Union[xr.Dataset, Path]: The result of processing the key according to the selected method.
        """
        print(f"Processing {key}")
        cat = self.cat_dict[key]
        dst = self.open_and_chunk_dataset(cat)

        if self.method == 2:
            return self.process_method_2(dst, cat, key)
        else:
            return self.process_method_1(dst, cat, key)

    def open_and_chunk_dataset(self, cat: dict) -> None:
        """
        Open and chunk the dataset based on the provided catalog information.

        Args:
            cat (dict): The catalog information.

        Returns:
            xarray.Dataset: The opened and chunked dataset.
        """
        return xr.open_dataset(
            cat.get("URL"),
            chunks={
                cat.get("T_name"): "auto",
                cat.get("Y_name"): "auto",
                cat.get("X_name"): "auto",
                "ens": "auto",
            },
        )

    def process_method_1(self, dst: xr.Dataset, cat: dict, key: str) -> Path:
        """
        Process the median ensemble data.

        Args:
            dst (xr.Dataset): The dataset to process.
            cat (dict): The catalog information.
            key (str): The key for processing.

        Returns:
            Path: The file path of the processed data.
        """
        ds = self.subset_xarray_by_geodf_bounds(xr_dataset=dst, cat=cat)
        median = ds[key].median(dim="ens")
        median.attrs = ds[key].attrs
        median_computed = median.compute().transpose("time", "lat", "lon")

        user_data = self.create_user_cat_data(median_computed.to_dataset(), cat, key)
        agg_gen = self.create_agg_gen(user_data, key)
        f_path = self.optpath / f"{self.fileprefix}{key}.nc"
        _ngdf, _ds_out = agg_gen.calculate_agg()

        return f_path

    def subset_xarray_by_geodf_bounds(self, xr_dataset, cat):
        """
        Subsets an xarray dataset to the bounds of a GeoPandas DataFrame using dynamic dimension names.

        Args:
        xr_dataset (xr.Dataset): The xarray dataset to be subset.
        gdf (gpd.GeoDataFrame): The GeoDataFrame whose bounds will be used to subset the dataset.
        cat (object): An object that provides dimension names dynamically, typically using a .get() method.

        Returns:
        xr.Dataset: A subset of the original xarray dataset.
        """
        # This method has some hard-coding but this is probably safe because it's focused on a a known dataset.
        # Get the bounding box coordinates from the GeoDataFrame
        buffer_deg = 0.04167
        minx, miny, maxx, maxy = self.gdf.to_crs(cat.get("crs")).total_bounds
        buffered_minx = minx - buffer_deg
        buffered_miny = miny - buffer_deg
        buffered_maxx = maxx + buffer_deg
        buffered_maxy = maxy + buffer_deg

        # Retrieve dimension names dynamically
        lat_name = cat.get("Y_name")
        lon_name = cat.get("X_name")

        # Subset the xarray dataset using .sel method with slice
        subset_ds = xr_dataset.sel(
            {
                lon_name: slice(buffered_minx, buffered_maxx),
                lat_name: slice(buffered_maxy, buffered_miny),
            }
        )

        return subset_ds

    def process_method_2(self, dst: xr.Dataset, cat: dict, key: str) -> xr.Dataset:
        """
        Process the ensemble data.

        Args:
            dst (xr.Dataset): The dataset to process.
            cat (dict): The catalog information.
            key (str): The key for processing.

        Returns:
            xr.Dataset: The combined dataset after processing.
        """
        ens_list = self.process_ensemble(dst, cat, key)
        datasets = [xr.open_dataset(file) for file in ens_list]
        combined_ds = xr.concat(datasets, dim="ens", combine_attrs="override")
        combined_ds = combined_ds.assign_coords(ens=np.arange(len(ens_list)))

        return combined_ds

    def process_ensemble(self, dst: xr.Dataset, cat: dict, key: str) -> List[Path]:
        """
        Process the ensemble data for a specific key.

        Args:
            dst (xr.Dataset): The dataset to process.
            cat (dict): The catalog information.
            key (str): The key for processing.

        Returns:
            List[Path]: A list of paths to the processed ensemble data files.
        """
        ens_list = []
        for n in dst.ens.values:
            new_ds = dst.sel(ens=n).transpose("time", "lat", "lon")
            user_data = self.create_user_cat_data(new_ds, cat, key)
            agg_gen = self.create_agg_gen(user_data, key, ensemble=n)
            _ngdf, _ds_out = agg_gen.calculate_agg()
            ens_file = self.optpath / f"{self.fileprefix}{key}_{int(n)}.nc"
            ens_list.append(ens_file)

        self.proc_median_files.extend(ens_list)
        return ens_list

    def create_user_cat_data(self, ds: xr.Dataset, cat: dict, key: str) -> UserCatData:
        """
        Create UserCatData object based on the provided dataset and catalog information.

        Args:
            ds (xr.Dataset): The dataset to use.
            cat (dict): The catalog information.
            key (str): The key for the data.

        Returns:
            UserCatData: The created UserCatData object.
        """
        return UserCatData(
            ds=ds,
            proj_ds=cat.get("crs"),
            x_coord=cat.get("X_name"),
            y_coord=cat.get("Y_name"),
            t_coord=cat.get("T_name"),
            var=[key],
            f_feature=self.gdf,
            proj_feature=self.gdf.crs,
            id_feature=self.feature_id,
            period=[self.start_date, self.end_date],
        )

    def create_agg_gen(
        self, user_data: UserCatData, key: str, ensemble: int = None
    ) -> AggGen:
        """
        Create an AggGen object for weighted aggregation.

        Args:
            user_data (UserCatData): The user data for aggregation.
            key (str): The key for the data.
            ensemble (int, optional): The ensemble number. Defaults to None.

        Returns:
            AggGen: The AggGen object for weighted aggregation.
        """
        file_suffix = f"_{int(ensemble)}" if ensemble is not None else ""
        return AggGen(
            user_data=user_data,
            stat_method="mean",
            agg_engine="serial",
            agg_writer="netcdf",
            weights=str(self.wghts_file),
            out_path=str(self.optpath),
            file_prefix=self.fileprefix + key + file_suffix,
        )

    def process_dataset(
        self,
        ds: xr.Dataset,
        var_rename: dict,
        feature_id_rename: dict,
        conversion_path: Path,
        fill_path: Path,
        n: int = -1,
    ) -> None:
        """
        Process the dataset by renaming variables, converting units, and writing to a NetCDF file.

        Args:
            ds (xr.Dataset): The dataset to process.
            var_rename (dict): Dictionary for renaming variables.
            feature_id_rename (dict): Dictionary for renaming feature IDs.
            conversion_path (Path): The path for the converted NetCDF file.
            fill_path (Path): The path output the file generated from filling missing values, This is the final file.
            n (int, optional): The ensemble number. Defaults to 0.
        """
        ds = (
            ds.rename_vars(var_rename)
            .rename_dims(feature_id_rename)
            .rename(feature_id_rename)
        )
        ds = (
            ds.pint.quantify({"lat": None, "lon": None})
            .pint.to({"tmax": "degC", "tmin": "degC"})
            .pint.dequantify()
        )
        ds.to_netcdf(conversion_path, format="NETCDF4", engine="netcdf4")
        print(f"converted file written to {conversion_path}")
        if self.fillmissing:
            self.fill_missing_values(conversion_path, fill_path, n)

    def fill_missing_values(self, conv_f: Path, output_dir: Path, n: int = -1) -> None:
        """
        Fill missing values in the converted file and handle renaming if necessary.

        Args:
            conv_f (Path): The path to the converted file.
            output_dir (Path): The output directory for the filled file.
            n (int, optional): The ensemble number. Defaults to 0.
        """
        print("filling missing values")
        response = fill_onhm_ncf(
            nfile=conv_f,
            output_dir=output_dir,
            mfile="filltest.csv",
            var="tmax",
            lat="lat",
            lon="lon",
            feature_id="nhru",
            genmap=True,
            mode="cfsv2",
            ensemble=n
        )
        if not response:
            new_name = (
                output_dir
                / f"{self.start_date}_filled_converted_{int(n) if self.method == 2 else 'median'}.nc"
            )
            conv_f.rename(new_name)
            print(f"No missing values, converted file renamed to: {new_name}")
        print(
            f"finished filling missing values for {f'ensemble:{str(n)}' if self.method == 2 else 'median'}"
        )

    def clean_intermediate_files(self):
        """
        Clean up intermediate files in the specified directory.

        This function iterates over the files in the directory and removes any files with the '.nc' extension.
        """
        print("Cleaning intermediate files.")
        for item in self.optpath.iterdir():
            if item.is_file() and item.name.endswith(".nc"):
                try:
                    item.unlink()
                except Exception as e:
                    print(f"Error removing {item}: {e}")

    def finalize(self) -> None:
        """
        Finalize the data processing by handling dataset based on the specified method.

        Args:
            self: The instance of the class.

        Returns:
            None
        """
        f_path = self.optpath / f"{self.fileprefix}.nc"
        ds = xr.open_dataset(f_path)
        var_rename = {"tmmx": "tmax", "tmmn": "tmin", "pr": "prcp"}
        feature_id_rename = {self.feature_id: "nhru"}

        if self.method == 2:
            for n in ds.ens.values:
                dst = ds.sel(ens=n)
                conv_f = self.optpath / f"{self.start_date}_converted_{int(n)}.nc"
                ensemble_n_path = ensure_directory(self.ensemble_path / f"ensemble_{int(n)}")
                self.process_dataset(
                    dst, var_rename, feature_id_rename, conv_f, ensemble_n_path, n
                )
        elif self.method == 1:
            conv_f = self.optpath / f"{self.start_date}_converted.nc"
            # median_path = self.ensure_directory(self.optpath / "ensemble_median" / self.start_date)
            self.process_dataset(
                ds, var_rename, feature_id_rename, conv_f, self.median_path
            )

        self.clean_intermediate_files()

    def read_target_based_on_suffix(self):
        """
        Reads a file based on its suffix (extension).
        If the file has a '.parquet' extension, it reads it as a Parquet file.
        If the file has a '.shp' extension, it reads it as a shapefile.
        """
        # file_suffix = self.target_file.lower().split(".")[-1]
        file_suffix = self.target_file.suffix.lower()[1:]

        if file_suffix == "parquet":
            # Read the file as a Parquet file
            self.gdf = gpd.read_parquet(self.target_file)  # , engine='auto')
        elif file_suffix == "shp":
            # Read the file as a shapefile
            self.gdf = gpd.read_file(self.target_file)
        else:
            print(
                f"Unsupported file format: {file_suffix}. Please provide a Parquet or shapefile."
            )


class GridMetETL:
    """Class for fetching climate data and parsing into netcdf."""

    def __init__(self):
        """Initialize class."""

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
        target_file: Path,
        optpath: Path,
        weights_file: Path,
        feature_id: str,
        start_date: str,
        end_date: str,
        fileprefix: str = "",
        partial: bool = False,
        fillmissing: bool = False,
    ):
        """
        Initialize the ETL process with specified parameters and validate file paths.

        Args:
            target_file (Path): Path to the target file.
            optpath (Path): Path to the output directory.
            weights_file (Path): Path to the weights file.
            feature_id (str): Identifier for the feature.
            start_date (str): Start date for processing.
            end_date (str): End date for processing.
            fileprefix (str, optional): Prefix for output files. Defaults to "".
            partial (bool, optional): Indicates if the process is partial or complete. Defaults to False.
            fillmissing (bool, optional): Indicates if missing values should be filled. Defaults to False.

        Returns:
            bool: True if initialization is successful, False otherwise.

        Raises:
            SystemExit: If input path, output path, or weights file does not exist.
        """

        self.feature_id = feature_id
        self.partial = partial
        self.fillmissing = fillmissing
        # Check input paths
        self.target_file = target_file
        self.optpath = optpath
        self.wghts_file = weights_file

        self.start_date = start_date
        self.end_date = end_date
        print(self.end_date, type(self.end_date))
        self.fileprefix = fileprefix

        print(Path.cwd())

        print(
            f"start_date: {self.start_date} and end_date: {self.end_date}",
            flush=True,
        )

        self.read_target_based_on_suffix()
        print(f"The source file: {self.target_file}", flush=True)
        print(f"the shapefile header is: {self.gdf.head()}", flush=True)

        # Download netcdf subsetted data
        data_id = "gridmet"
        try:
            with pkg_resources.open_binary(
                "gridmet_etl.data", "catalog.parquet"
            ) as file:
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
        """
        Run the weighted aggregation process based on the specified method.

        Args:
            self: The instance of the class.

        Returns:
            tuple: A tuple containing the new GeoDataFrame (ngdf) and the output dataset (ds_out).
        """

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
        """
        Finalize the data processing by renaming variables, converting units, and filling missing data if required.

        Args:
            self: The instance of the class.

        Returns:
            None
        """
        print(Path.cwd(), flush=True)
        f_path = self.optpath / f"{self.fileprefix}.nc"
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
            self._fill_missing(conv_f)

    def _fill_missing(self, conv_f):
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
            mode="gridmet"
        )
        if not response:
            new_name = (
                self.optpath / f"{self.start_date}_converted_filled.nc"
            )
            conv_f.rename(new_name)
            print(f"No missing values, converted file renamed to: {new_name}")
        print("finished filling missing values")
        print("Cleaning intermediate files.")
        # Iterate over each item in the directory
        for item in self.optpath.iterdir():
            # Check if the item is a file and ends with '.nc'
            if (
                item.is_file()
                and item.name.endswith(".nc")
                and "filled" not in item.name
            ):
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
        # file_suffix = self.target_file.lower().split(".")[-1]
        file_suffix = self.target_file.suffix.lower()[1:]
        if file_suffix == "parquet":
            # Read the file as a Parquet file
            self.gdf = gpd.read_parquet(self.target_file)  # , engine='auto')
        elif file_suffix == "shp":
            # Read the file as a shapefile
            self.gdf = gpd.read_file(self.target_file)
        else:
            print(
                f"Unsupported file format: {file_suffix}. Please provide a Parquet or shapefile."
            )
