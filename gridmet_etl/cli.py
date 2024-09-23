"""Console script for gridmetetl."""

from cyclopts import App, Group, Parameter, validators
from typing_extensions import Annotated
from gridmet_etl.etl import GridMetETL, CFSv2ETL
import argparse
import sys
import datetime
from pathlib import Path

app = App()


def valid_date(_type, value) -> None:
    try:
        return datetime.datetime.strptime(value, "%Y-%m-%d")
    except ValueError as e:
        msg = "Not a valid date: '{0}'.".format(value)
        raise argparse.ArgumentTypeError(msg) from e

def valid_path(_type, value):
    if Path(value).exists():
        return Path(value)
    else:
        raise argparse.ArgumentTypeError(f"Path does not exist: {value}")

@app.command
def gridmet_etl(
    start_date: Annotated[
        str,
        Parameter(
            validator=valid_date,
            help="Start date of retrieval (YYYY-MM-DD)",
            required=True,
        ),
    ],
    end_date: Annotated[
        str,
        Parameter(
            validator=valid_date,
            help="Start date of retrieval (YYYY-MM-DD)",
            required=True,
        ),
    ],
    file_prefix: Annotated[
        str,
        Parameter(
            help="Prefix for output files",
            required=True
        )          
    ],
    target_file: Annotated[
        Path,
        Parameter(
            validator=valid_path,
            help="Input geometry file (target polygon geometry file: read by geopandas)",
            required=True
        )
    ],
    output_path: Annotated[
        Path,
        Parameter(
            validator=valid_path,
            help="Output path (location of netcdf output files by shapefile output)",
            required=True
        )
    ],
    weight_file: Annotated[ 
        Path,
        Parameter(
            validator=valid_path,
            help="path/weight.csv - path/name of weight file",
            required=True
        )
    ],
    fid: Annotated[ 
        str,
        Parameter(
            help="Target file column id to identify results",
            required=True
        )
    ],
    partial: Annotated[
        bool,
        Parameter(
            help="option: set if you expect only partial mapping to HRU and you want the partial mapping",
            required=False
        )
    ] = False,
    fill_missing: Annotated[
        bool,
        Parameter(
           help="option: fill outside conus hrus according to nearest neighbor using mapping file",
           required=False 
        )
    ] = False

):
    """
    Retrieves and processes environmental data within a specified date range and geographical area, 
    then outputs the results as netCDF files.

    Args:
        start_date (str): The start date for data retrieval in YYYY-MM-DD format. This parameter is required.
        end_date (str): The end date for data retrieval in YYYY-MM-DD format. This parameter is required.
        file_prefix (str): The prefix to be used for naming output files. This should be a valid path. This parameter is required.
        target_file (Path): The path to the input geometry file. This file should contain the target polygon geometries and must be readable by geopandas. This parameter is required.
        output_path (Path): The directory path where the netCDF output files will be saved. This should be a valid path. This parameter is required.
        weight_file (Path): The path to the weight file (typically named 'weight.csv'), which contains weighting information for the data processing. This should be a valid path. This parameter is required.
        fid (str): The column identifier in the target file used to identify results. This parameter is required.

    Returns:
        None: The function processes data and writes the output to netCDF files at the specified output location.

    Raises:
        ValueError: If any of the provided dates are invalid or not in the required format.
        FileNotFoundError: If any of the provided file paths do not exist.
        Exception: For any other issues that arise during the processing of the data.
    """
    print("starting Script", flush=True)
    fp = GridMetETL()
    print(f"Partial = {partial}")
    print("instantiated", flush=True)
    try:
        fp.initialize(
            target_file=target_file,
            optpath=output_path,
            weights_file=weight_file,
            feature_id=fid,
            start_date=start_date,
            end_date=end_date,
            fileprefix=file_prefix,
            partial=partial,
            fillmissing=fill_missing,
        )
        print("initalized\n", flush=True)
        print("running", flush=True)
        fp.run_weights()
        print("finished running", flush=True)
        fp.finalize()
        print("finalized", flush=True)

    except Exception as error:
        print(f"An error: {error}")

@app.command
def cfsv2_etl(
    file_prefix: Annotated[
        str,
        Parameter(
            help="Prefix for output files",
            required=True
        )          
    ],
    target_file: Annotated[
        Path,
        Parameter(
            validator=valid_path,
            help="Input geometry file (target polygon geometry file: read by geopandas)",
            required=True
        )
    ],
   fid: Annotated[ 
        str,
        Parameter(
            help="Target file column id to identify results",
            required=True
        )
    ],
    output_path: Annotated[
        Path,
        Parameter(
            validator=valid_path,
            help="Output path (location of netcdf output files by shapefile output)",
            required=True
        )
    ],
    weight_file: Annotated[ 
        Path,
        Parameter(
            validator=valid_path,
            help="path/weight.csv - path/name of weight file",
            required=True
        )
    ],
    model_param_file: Annotated[ 
        Path,
        Parameter(
            validator=valid_path,
            help="path/myparam.param - path/name of model parameter file containing elevation per hru",
            required = True
        )
    ],
    method: Annotated[
        int,
        Parameter(
            help="Choice of (1) median or (2) ensemble",
            required=True
        )
    ] = 1,
    partial: Annotated[
        bool,
        Parameter(
            help="option: set if you expect only partial mapping to HRU and you want the partial mapping",
            required=False
        )
    ] = False,
    fill_missing: Annotated[
        bool,
        Parameter(
           help="option: fill outside conus hrus according to nearest neighbor using mapping file",
           required=False 
        )
    ] = False
) -> None:
    print("starting Script", flush=True)
    cfs = CFSv2ETL()
    print(f"Partial = {partial}")
    print("instantiated", flush=True)
    try:
        cfs.initialize(
            target_file=target_file,
            optpath=output_path,
            weights_file=weight_file,
            elevation_file=model_param_file,
            feature_id=fid,
            method=method,
            fileprefix=file_prefix,
            partial=partial,
            fillmissing=fill_missing
        )
        print("initalized\n", flush=True)
        print("running", flush=True)
        cfs.run_weights()
        print("finished running", flush=True)
        cfs.finalize()
        print("finalized", flush=True)
    except Exception as error:
        print(f"An error: {error}")

def main():
    try:
        app()
    except Exception as e:
        print(e)

if __name__ == "__main__":
    sys.exit(main())
