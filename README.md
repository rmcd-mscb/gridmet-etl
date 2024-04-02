# gridmet-etl

A python package and CLI for processing area-weighted spatial interpolations of gridmet climate drivers using gdptools.  This package is intended to be used for processing gridmet climate drivers for the U.S. Geolocical Survey's Operational National Hydrologic Model.  

Currently the packages supports processing [gridmet](https://www.climatologylab.org/gridmet.html) and downscaled [CFSv2](http://thredds.northwestknowledge.net:8080/thredds/catalog/NWCSC_INTEGRATED_SCENARIOS_ALL_CLIMATE/cfsv2_metdata_90day/catalog.html).  For the latter it processes the 48 day ensemble versions and can process either the ensemble median, or all 48 ensembles.  See below for examples.

## Install

To install the package into a local conda env, run the following commands.

```console
conda env create -f environment.yml
conda activate gridmet-etl
poetry install
```

## Example code usage

To see available commands, run `gridmetetl -h`

```console
& gridmetetl -h
Usage: gridmetetl COMMAND

╭─ Commands ──────────────────────────────────────────────────────────────╮
│ cfsv2-etl                                                               │
│ gridmet-etl                                                             │
│ --help,-h    Display this message and exit.                             │
│ --version    Display application version.                               │
╰─────────────────────────────────────────────────────────────────────────╯
```

### gridmet

To see the interface for running a gridmet-etl command, run `gridmetetl gridmet-etl -h`

```console
sage: gridmetetl gridmet-etl [ARGS] [OPTIONS]

Retrieves and processes environmental data within a specified date range
and geographical area, then outputs the results as netCDF files.

╭─ Parameters ────────────────────────────────────────────────────────────╮
│ *  START-DATE,--start-date                        Start date of         │
│                                                   retrieval             │
│                                                   (YYYY-MM-DD)          │
│                                                   [required]            │
│ *  END-DATE,--end-date                            Start date of         │
│                                                   retrieval             │
│                                                   (YYYY-MM-DD)          │
│                                                   [required]            │
│ *  FILE-PREFIX,--file-prefix                      Prefix for output     │
│                                                   files [required]      │
│ *  TARGET-FILE,--target-file                      Input geometry file   │
│                                                   (target polygon       │
│                                                   geometry file: read   │
│                                                   by geopandas)         │
│                                                   [required]            │
│ *  OUTPUT-PATH,--output-path                      Output path (location │
│                                                   of netcdf output      │
│                                                   files by shapefile    │
│                                                   output) [required]    │
│ *  WEIGHT-FILE,--weight-file                      path/weight.csv -     │
│                                                   path/name of weight   │
│                                                   file [required]       │
│ *  FID,--fid                                      Target file column id │
│                                                   to identify results   │
│                                                   [required]            │
│    PARTIAL,--partial,--no-partial                 option: set if you    │
│                                                   expect only partial   │
│                                                   mapping to HRU and    │
│                                                   you want the partial  │
│                                                   mapping [default:     │
│                                                   False]                │
│    FILL-MISSING,--fill-missing,--no-fill-missing  option: fill outside  │
│                                                   conus hrus according  │
│                                                   to nearest neighbor   │
│                                                   using mapping file    │
│                                                   [default: False]      │
╰─────────────────────────────────────────────────────────────────────────╯
```

* example command:

  ```console
  gridmetetl gridmet-etl --start-date 1980-01-01 --end-date 1980-01-07 --file-prefix gm_ --target-file ./data/nhru_01a/nhru_01a.parquet --output-path ./data/output --weight-file ./data/nhru_01a/gm_nhru01_weights.csv --fid nhru_v1_1 --fill-missing --no-partial
  ```

* result: This results in a file named data/output/1980-01-01_converted_filled.nc
* [result header](gm_header.md)

### cfsv2

To see the interface for running a cfsv2-etl command, run `gridmetetl cfsv2-etl -h`

```console
Usage: gridmetetl cfsv2-etl [ARGS] [OPTIONS]

╭─ Parameters ────────────────────────────────────────────────────────────────────╮
│ *  FILE-PREFIX,--file-prefix                      Prefix for output files       │
│                                                   [required]                    │
│ *  TARGET-FILE,--target-file                      Input geometry file (target   │
│                                                   polygon geometry file: read   │
│                                                   by geopandas) [required]      │
│ *  FID,--fid                                      Target file column id to      │
│                                                   identify results [required]   │
│ *  OUTPUT-PATH,--output-path                      Output path (location of      │
│                                                   netcdf output files by        │
│                                                   shapefile output) [required]  │
│ *  WEIGHT-FILE,--weight-file                      path/weight.csv - path/name   │
│                                                   of weight file [required]     │
│ *  METHOD,--method                                Choice of (1) median or (2)   │
│                                                   ensemble [required]           │
│    PARTIAL,--partial,--no-partial                 option: set if you expect     │
│                                                   only partial mapping to HRU   │
│                                                   and you want the partial      │
│                                                   mapping [default: False]      │
│    FILL-MISSING,--fill-missing,--no-fill-missing  option: fill outside conus    │
│                                                   hrus according to nearest     │
│                                                   neighbor using mapping file   │
│                                                   [default: False]              │
╰─────────────────────────────────────────────────────────────────────────────────╯
```

#### Method: Ensemble_median

This produces an interpolation of the median of all 48 ensembles for tmax, tmin, and precip. The file prefix is the start_date of the forecast.  For example `2024-04-01_converted_filled.nc`

* example command using `--method 1` for the ensemble median:

  ```console
  gridmetetl cfsv2-etl --file-prefix cfsv2_ --target-file ./data/nhru_01a/nhru_01a.parquet --output-path ./data/output --weight-file ./data/nhru_01a/gm_nhru01_weights.csv --fid nhru_v1_1 --method 1 --fill-missing --no-partial
  ```

* result: This results in a file named data/output/median_ensemble/2024-04-01_converted_filled.nc
* [result header](cfsv2_median_header.md)

#### Method: Ensemble

This produces an interpolation for each of the 48 ensembles for tmax, tmin, precip.  The file prefix is the start_date of the forecast and the file post-fix is the ensemble number, for esample: `2024=04-01_converted_filled_0.nc`

* example command using `--method 2` for the ensemble median:

  ```console
  gridmetetl cfsv2-etl --file-prefix cfsv2_ --target-file ./data/nhru_01a/nhru_01a.parquet --output-path ./data/output --weight-file ./data/nhru_01a/gm_nhru01_weights.csv --fid nhru_v1_1 --method 2 --fill-missing --no-partial
  ```

* result: This results in a file named data/output/ensembles/2024-04-01_converted_filled_0.nc
* [result header](cfsv2_ensemble_header.md)
