# NetCDF File Header

For file data/output/1980-01-01_converted_filled

```console
    netcdf \1980-01-01_converted_filled {

    dimensions:
            time = 7 ;
            nhru = 2462 ;
    variables:
            double tmax(time, nhru) ;
                    tmax:_FillValue = 9.96920996838687e+36 ;
                    tmax:long_name = "tmmx" ;
                    tmax:grid_mapping = "crs" ;
                    tmax:units = "degree_Celsius" ;
                    tmax:coordinates = "time lat lon" ;
            double crs ;
                    crs:_FillValue = NaN ;
                    crs:crs_wkt = "GEOGCRS[\"WGS 84\",ENSEMBLE[\"World Geodetic System 1984 ensemble\",MEMBER[\"World Geodetic System 1984 (Transit)\"],MEMBER[\"World Geodetic System 1984 (G730)\"],MEMBER[\"World Geodetic System 1984 (G873)\"],MEMBER[\"World Geodetic System 1984 (G1150)\"],MEMBER[\"World Geodetic System 1984 (G1674)\"],MEMBER[\"World Geodetic System 1984 (G1762)\"],MEMBER[\"World Geodetic System 1984 (G2139)\"],ELLIPSOID[\"WGS 84\",6378137,298.257223563,LENGTHUNIT[\"metre\",1]],ENSEMBLEACCURACY[2.0]],PRIMEM[\"Greenwich\",0,ANGLEUNIT[\"degree\",0.0174532925199433]],CS[ellipsoidal,2],AXIS[\"geodetic latitude (Lat)\",north,ORDER[1],ANGLEUNIT[\"degree\",0.0174532925199433]],AXIS[\"geodetic longitude (Lon)\",east,ORDER[2],ANGLEUNIT[\"degree\",0.0174532925199433]],USAGE[SCOPE[\"Horizontal component of 3D system.\"],AREA[\"World.\"],BBOX[-90,-180,90,180]],ID[\"EPSG\",4326]]" ;
                    crs:semi_major_axis = 6378137. ;
                    crs:semi_minor_axis = 6356752.31424518 ;
                    crs:inverse_flattening = 298.257223563 ;
                    crs:reference_ellipsoid_name = "WGS 84" ;
                    crs:longitude_of_prime_meridian = 0. ;
                    crs:prime_meridian_name = "Greenwich" ;
                    crs:geographic_crs_name = "WGS 84" ;
                    crs:horizontal_datum_name = "World Geodetic System 1984 ensemble" ;
                    crs:grid_mapping_name = "latitude_longitude" ;
            double tmin(time, nhru) ;
                    tmin:_FillValue = 9.96920996838687e+36 ;
                    tmin:long_name = "tmmn" ;
                    tmin:grid_mapping = "crs" ;
                    tmin:units = "degree_Celsius" ;
                    tmin:coordinates = "time lat lon" ;
            double prcp(time, nhru) ;
                    prcp:_FillValue = 9.96920996838687e+36 ;
                    prcp:long_name = "pr" ;
                    prcp:grid_mapping = "crs" ;
                    prcp:units = "millimeter" ;
                    prcp:coordinates = "time lat lon" ;
            double rhmax(time, nhru) ;
                    rhmax:_FillValue = 9.96920996838687e+36 ;
                    rhmax:long_name = "rmax" ;
                    rhmax:grid_mapping = "crs" ;
                    rhmax:units = "percent" ;
                    rhmax:coordinates = "time lat lon" ;
            double rhmin(time, nhru) ;
                    rhmin:_FillValue = 9.96920996838687e+36 ;
                    rhmin:long_name = "rmin" ;
                    rhmin:grid_mapping = "crs" ;
                    rhmin:units = "percent" ;
                    rhmin:coordinates = "time lat lon" ;
            double ws(time, nhru) ;
                    ws:_FillValue = 9.96920996838687e+36 ;
                    ws:long_name = "vs" ;
                    ws:grid_mapping = "crs" ;
                    ws:units = "meter / second" ;
                    ws:coordinates = "time lat lon" ;
            int64 time(time) ;
                    time:units = "days since 1980-01-01 00:00:00" ;
                    time:calendar = "proleptic_gregorian" ;
            int64 nhru(nhru) ;
                    nhru:feature_id = "nhru_v1_1" ;
            double lat(nhru) ;
                    lat:long_name = "Latitude of HRU centroid" ;
                    lat:standard_name = "latitude" ;
                    lat:axis = "Y" ;
            double lon(nhru) ;
                    lon:long_name = "Longitude of HRU centroid" ;
                    lon:standard_name = "longitude" ;
                    lon:axis = "X" ;
            double humidity(time, nhru) ;
                    humidity:_FillValue = NaN ;
                    humidity:grid_mapping = "crs" ;
                    humidity:long_name = "Daily mean relative humidity" ;
                    humidity:units = "percent" ;
                    humidity:coordinates = "time lat lon" ;

    // global attributes:
                    :Conventions = "CF-1.8" ;
                    :featureType = "timeSeries" ;
                    :history = "2024_04_01_14_30_55 Original filec created  by gdptools package: https://code.usgs.gov/wma/nhgf/toolsteam/gdptools \n" ;
    }
  ```
