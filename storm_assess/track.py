""" 
Load function to read in Reading Universities TRACK output. 
Should work for individual ensemble member files and also the 
'combine' file. 

Note: All files need to contain the DATES of the storms, not the 
original timestep as output by TRACK.

Note: Any fields in addition to the full field vorticity, mslp
and 925 hPa max winds (e.g. 10m winds or precipitation) are not 
currently stored. If you want these values you need to read in 
the data file and include the variables in the 'extras' dictionary. 


"""
import gzip

import datetime
import cftime
import numpy as np
import pandas as pd
import xarray

from parse import parse

import storm_assess

# Use align specifications (^, <, >) to allow variable whitespace in headers
# Left aligned (<) for "nvars" so nfields takes all whitespace between in case there is
# only one space
# Right aligned (>) for variables at the end since lines are stripped of whitespace
# prior to parsing
header_fmt = "TRACK_NUM{ntracks:^d}ADD_FLD{nfields:^d}{nvars:<d}&{var_has_coords}"
track_header_fmt = "TRACK_ID{track_id:>d}"
track_header_fmt_new = "TRACK_ID{track_id:^d}START_TIME{start_time:>}"
track_info_fmt = "POINT_NUM{npoints:>d}"


def _parse(fmt, string, **kwargs):
    # Call parse but raise an error if None is returned
    result = parse(fmt, string, **kwargs)

    if result is None:
        raise ValueError(f"Format {fmt} does not match string {string}")

    return result


def load_netcdf(filename):
    """Load track data from netCDF file into a list of xarray datasets

    Args:
        filename (str):

    Returns:
        list:
    """
    ds = xarray.open_dataset(filename)

    # For some reason indexing included the "end" value in a slice if I don't drop these
    # variables first
    # e.g. ds.sel(record=slice(0, 10)) gives an dataset with 11 records rather than 10
    # I think it matches the numbers rather than the index so includes 10 as a match
    to_drop = []
    for name in ["tracks", "record"]:
        if name in ds:
            to_drop.append(name)
    ds = ds.drop_vars(names=to_drop)

    output = []
    for n, (idx0, npoints) in enumerate(zip(ds.FIRST_PT.data, ds.NUM_PTS.data)):
        track_da = ds.sel(record=slice(idx0, idx0 + npoints), tracks=n)
        track_da = track_da.swap_dims(record="time")
        track_da = track_da.drop_vars(names=["FIRST_PT", "NUM_PTS"])

        # Change scalar variables to attributes
        to_drop = []
        for varname in track_da:
            variable = track_da[varname]
            if variable.shape == ():
                if np.issubdtype(variable.dtype, np.datetime64):
                    track_da.attrs[varname] = pd.to_datetime(variable.data)
                else:
                    track_da.attrs[varname] = variable.data[()]
                to_drop.append(varname)
        track_da = track_da.drop_vars(names=to_drop)

        output.append(track_da)

    return output


def save_netcdf(tracks, filename):
    """

    Args:
        tracks (list): A list of xarray.Dataset, each representing an individual track,
             such as output from load_no_assumptions or load_netcdf
        filename (str):
    """
    new_tracks = []

    first_point = xarray.DataArray(
        data=np.zeros(len(tracks), dtype=int),
        dims=["tracks"],
        coords=dict(tracks=np.arange(len(tracks), dtype=int)),
        name="FIRST_PT",
    )
    num_pts = xarray.DataArray(
        data=np.zeros(len(tracks), dtype=int),
        dims=["tracks"],
        coords=dict(tracks=np.arange(len(tracks), dtype=int)),
        name="NUM_PTS",
    )

    idx = 0
    for n, tr in enumerate(tracks):
        first_point[n] = idx
        num_pts[n] = len(tr.time)

        record = xarray.DataArray(
            data=np.array(range(idx, idx + len(tr.time))),
            dims=["time"],
            coords=dict(time=tr.time),
        )

        new_tr = tr.assign(record=record).swap_dims(time="record").reset_coords(names="time")

        new_tracks.append(new_tr)

        idx += len(tr.time)

    new_tracks = xarray.concat(new_tracks, dim="record", combine_attrs="drop_conflicts")

    # Dropped attributes are attributes that differ between tracks so should be re-added
    # as variables along the tracks dimension
    # Get original attributes and convert to lists
    attrs = [tr.attrs for tr in tracks]
    keys = set([key for d in attrs for key in d])
    for key in keys:
        attrs_as_list = [d[key] if key in d else None for d in attrs]
        new_tracks[key] = (["tracks"], attrs_as_list)

    new_tracks = new_tracks.assign(FIRST_PT=first_point, NUM_PTS=num_pts)

    new_tracks.to_netcdf(filename)


def load_no_assumptions(filename, calendar=None, variable_names=None):
    """Load track data as xarray Datasets with generic names for added variables

    Args:
        filename (str):
        calendar (str, optional):
        variable_names(list, optional): A list of the names of additional variables
            present in the file. If None, the variables will be named as variable_n, and
            associated coordinates as variable_n_latitude/variable_n_longitude

    Returns:
        list:
    """
    output = list()

    if filename.split(".")[-1] == "gz":
        open_func = gzip.open
    else:
        open_func = open

    with open_func(filename, "rt") as f:
        # The first lines can contain extra information bounded by two extra lines
        # Just skip to the main header line for now
        line = ""
        while not line.startswith('TRACK_NUM'):
            line = f.readline().strip()

        # Load information about tracks from header line
        # If there are no added variables the line ends at the "&"
        try:
            header = _parse(header_fmt, line).named
        except ValueError:
            header = _parse(header_fmt.split("&")[0] + "&", line).named
            header["var_has_coords"] = ""

        ntracks = header["ntracks"]
        nfields = header["nfields"]
        nvars = header["nvars"]
        has_coords = [int(x) == 1 for x in header["var_has_coords"]]

        # Check header data is consistent
        assert len(has_coords) == nfields
        assert sum([3 if x == 1 else 1 for x in has_coords]) == nvars

        # Create a list of variables stored in each track
        # Generic names for variables as there is currently no information otherwise
        var_labels = ["longitude", "latitude", "vorticity"]
        for n in range(nfields):
            if has_coords[n]:
                var_labels.append(f"feature_{n}_longitude")
                var_labels.append(f"feature_{n}_latitude")
            var_labels.append(f"feature_{n}")

        # Read in each track as an xarray dataset with time as the coordinate
        for n in range(ntracks):
            # Read individual track header (two lines)
            line = f.readline().strip()
            try:
                track_info = _parse(track_header_fmt, line).named
            except ValueError:
                track_info = _parse(track_header_fmt_new, line).named
                track_info["start_time"] = parse_date(
                    track_info["start_time"], calendar=calendar
                )

            line = f.readline().strip()
            npoints = _parse(track_info_fmt, line)["npoints"]

            # Generate arrays for time coordinate and variables
            # Time is a list because it will hold datetime or cftime objects
            # Other variables are a dictionary mapping variable name to a tuple of
            # (time, data_array) as this is what is passed to xarray.Dataset
            times = [None] * npoints
            track_data = {label: ("time", np.zeros(npoints)) for label in var_labels}

            # Populate time and data line by line
            for m in range(npoints):
                line = f.readline().strip().split("&")
                time, lon, lat, vorticity = line[0].split()
                times[m] = parse_date(time, calendar=calendar)
                track_data["longitude"][1][m] = float(lon)
                track_data["latitude"][1][m] = float(lat)
                track_data["vorticity"][1][m] = float(vorticity)

                for i, label in enumerate(var_labels[3:]):
                    track_data[label][1][m] = float(line[i+1])

            # Return a dataset for the individual track
            output.append(xarray.Dataset(
                track_data,
                coords=dict(time=times),
                attrs=track_info,
            ))

    if variable_names is not None:
        return rename_tracks(output, variable_names)
    else:
        return output


def rename_tracks(tracks, new_names):
    """Add variable names to tracks loaded by load_no_assumptions

    By default, load_no_assumptions will load TRACK files with additional variables just
    labelled as "variable_n" and "variable_n_latitude"/"variable_n_longitude" if they
    have associated coordinates. This function renames those variables to the names
    passed in as new_names

    Args:
        tracks (list of xarray.Dataset): List of tracks with extra unknown variables
        new_names (list of str): The list of new names with length equal to the number
            of extra variables in the tracks

    Returns:
        list of xarray.Dataset: The input tracks with the variables renamed
    """
    # Extract variable names that need changing in the tracks
    # Use the first track as it is assumed they have all come from the same set of
    # data
    # With load_no_assumptions the extra variables are listed as feature_n and if they
    # have a lat/lon association, also feature_n_latitude and feature_n_longitude
    to_rename = [var for var in list(tracks[0]) if "feature" in var]

    # Map the variables that need renaming to the new names
    mapping = dict()
    for var in to_rename:
        # Split into [variable, n, "longitude"/"latitude"]
        elements = var.split("_")
        new_name = new_names[int(elements[1])]
        # If longitude/latitude is in the name, include it in the new name
        if len(elements) > 2:
            new_name += "_" + "_".join(elements[2:])

        mapping[var] = new_name

    # Rename all the tracks
    return [tr.rename(mapping) for tr in tracks]


def load(fh, ex_cols=0, calendar=None):
    """
    Reads model tropical storm tracking output from Reading Universities TRACK
    algorithm. Note: lat, lon, vorticity, maximum wind speed and minimum central
    pressure values are taken from the full resolution field, not T42. The lat/
    lon values correspond to the location of maximum 850 hPa relative vorticity
    (unless data are unavailable, in which case the original T42 resolution lat/
    lon values are used).

    If you have additional fields/columns after the full field vorticity, max
    wind and mslp data, then you need to count the number of columms these
    take up and set this as the ex_cols value (this value does not include
    &'s or comma's, just data). For example, for a file containing additional
    10m wind information ex_cols=3 (lat, lon, 10m wind speed value).

    Note: if you are using model data which uses a 12 months x 30 daycalendar
    then you need to set calendar to 'netcdftime'. Default it to use gregorian
    calendar.


    IMPORTANT NOTE:
    This funciton assumes that added fields are, in order, the full-field vorticity (7 levels),
    MSLP, 925hPa wind speed, and 10m wind speed.


    """
    # allow users to pass a filename instead of a file handle.
    if isinstance(fh, str):
        with open(fh, 'r') as fh:
            for data in load(fh, ex_cols=ex_cols, calendar=calendar):
                yield data

    else:
        # for each line in the file handle
        for line in fh:
            if line.startswith('TRACK_NUM'):
                header_line = line.split()
                if header_line[2] == 'ADD_FLD':
                    number_fields = int(header_line[3])
                else:
                    raise ValueError('Unexpected line in TRACK output file.')
                # if no of fields is 9 then we have the mslp and wind, and ex_cols=3, else not
                #nlevels_t63 = number_fields - 4
                #if number_fields == 7 or number_fields == 9:
                #    ex_cols = 3
                if number_fields == 9:
                    ex_cols = 6
                else:
                    print('using ex_cols value ',ex_cols)
                    #ex_cols = 0
                # if no 10m wind, then only 2 extra fields
                if ex_cols == 3:
                    nlevels_t63 = number_fields - 2 - 1
                elif ex_cols == 6:
                    nlevels_t63 = number_fields - 2
                else:
                    nlevels_t63 = number_fields - 2

            if line.startswith('TRACK_ID'):
                # This is a new storm. Store the storm number.
                try:
                    _, snbr, _, _ = line.split()
                except:
                    _, snbr = line.split()
                snbr =  int(snbr.strip())

                # Now get the number of observation records stored in the next line
                next_line = next(fh)
                if next_line.startswith('POINT_NUM'):
                    _, n_records = next_line.split()
                    n_records = int(n_records)
                else:
                    raise ValueError('Unexpected line in TRACK output file.')

                # Create a new observations list
                storm_obs = []

                """ Read in the storm's observations """
                # For each observation record
                for _, obs_line in zip(list(range(n_records)), fh):

                    # Get each observation element
                    split_line = obs_line.strip().split('&')
                    storm_centre_record = split_line[0].split(' ')

                    # Get observation date and T42 lat lon location in case higher
                    # resolution data are not available
                    date, tmp_lon, tmp_lat, vort = split_line[0].split()
                    date = parse_date(date, calendar)

                    # Get storm location of maximum vorticity (full resolution field)
                    #lat = float(split_line[::-1][8+ex_cols])
                    #lon = float(split_line[::-1][9+ex_cols])
                    lat = float(storm_centre_record[2])
                    lon = float(storm_centre_record[1])

                    # Get full resolution mslp (hPa)
                    mslp = float(split_line[1+(3*nlevels_t63)+2])
                    if mslp > 1.0e4:
                        mslp /= 100
                    mslp = float(round(mslp,1))

                    # Get full resolution 925hPa maximum wind speed (m/s)
                    vmax = float(split_line[1+(3*nlevels_t63)+3+2])

                    # Check for mslp-vmax mix-up
                    if mslp < 500 and vmax > 500:
                        mslp,vmax = vmax,mslp

                    # Also store vmax in knots (1 m/s = 1.944 kts) to match observations
                    vmax_kts = vmax * 1.944

                    # Get full resolution 850 hPa maximum vorticity (s-1)
                    vort = float(storm_centre_record[3])
                    #vort = float(split_line[::-1][nlevels_t63+2+ex_cols])
                    #vort_idx = (nlevels_t63+2)*3
                    #vort = float(split_line[vort_idx])

                    # Get T63 vorticity
                    #t63_1 = float(split_line[::-1][(number_fields-2)*3+ex_cols+1])
                    #t63_2 = float(split_line[::-1][(number_fields-3)*3+ex_cols+1])
                    #t63_3 = float(split_line[::-1][(number_fields-4)*3+ex_cols+1])
                    #t63_4 = float(split_line[::-1][(number_fields-5)*3+ex_cols+1])
                    #t63_5 = float(split_line[::-1][(number_fields-nlevels_t63-1)*3+ex_cols+1])
                    #t63_diff = (t63_1 - t63_5)

                    # Get 10m wind speed
                    if ex_cols > 6:
                        v10m = float(split_line[::-1][1])
                        v10m_lat = float(split_line[::-1][2])
                        v10m_lon = float(split_line[::-1][3])

                    # If higher resolution lat/lon data is not available then use lat
                    # lon from T42 resolution data
                    if lat == 1e12 or lon == 1e12 or lat == 1.0e25 or lon == 1.0e25:
                        lat = float(tmp_lat)
                        lon = float(tmp_lon)

                    # Store observations
                    if ex_cols > 6:
                        storm_obs.append(storm_assess.Observation(date, lat, lon, vort, vmax, mslp,
                                         extras={'vmax_kts':vmax_kts,'v10m_lon':v10m_lon,'v10m_lat':v10m_lat,'v10m':v10m}))
                    else:
                        storm_obs.append(storm_assess.Observation(date, lat, lon, vort, vmax, mslp,
                                         extras={'vmax_kts':vmax_kts}))

                # Yield storm
                yield storm_assess.Storm(snbr, storm_obs, extras={})


def load_hart(fh, ex_cols=0, calendar=None):
    """
    Load function to read in University of Reading TRACK algorithm output.

    Notes:
        - All files should contain the DATES of the storms, not the
          original timesteps, as output by TRACK.

        - The fields read are:
            * full-field vorticity (7 isobaric levels)
            * mslp
            * 925 hPa max winds
            * 10m winds
            * Hart CPS parameters

        - Other added fields (e.g., precipitation) are not currently stored.
          To store these values, amend the 'extras' dictionary.

    Read tropical cyclone tracking output from University of Reading TRACK
    algorithm (Hodges, 1994; 1995; 1996).

    Note: lat, lon, vorticity, maximum wind speed and minimum central
    pressure values are taken from the full resolution field, not T42.
    The lat & lon values correspond to the location of maximum 850 hPa
    relative vorticity (unless full-field data are unavailable, in which
    case the original T42 resolution lat & lon values are used). This

    Note: assumes the added field order: vort, mslp, vmax, v10m, TL, TU, B
    If you have additional fields (columns), count the number of columms these
    occupy (or refer to the TRACK file header) and set 'ex_cols' to equal this
    value (excluding &s and commas). For example, for a file containing one
    additional field with positional information, ex_cols=3. Ammend 'extras'.

    Note: if using model data which uses a 12 months x 30 day calendar,
    set calendar to 'netcdftime'. Default is 'gregorian' calendar.
    """

    # allow users to pass a filename instead of a file handle.
    if isinstance(fh, str):
        with open(fh, 'r') as fh:
            for data in load(fh, ex_cols=ex_cols, calendar=calendar):
                yield data

    else:
        # for each line in the file handle
        for line in fh:
            if line.startswith('TRACK_NUM'):
                split_line = line.split()
                if split_line[2] == 'ADD_FLD':
                    number_fields = int(split_line[3])
                else:
                    raise ValueError('Unexpected line in TRACK output file.')
                # assume 7-level full-field vorticity,mslp,vmax,mslp,v10m,TL,TU,B
                if number_fields == 13:
                    ex_cols = 0
                    nlevels_T63 = 7
                else:
                    print(('using ex_cols = ', ex_cols))
                    nlevels_T63 = number_fields - 3

            # read storms
            if line.startswith('TRACK_ID'):
                # new storm: store the storm number
                try:
                    _, snbr, _, _ = line.split()
                except:
                    _, snbr = line.split()
                snbr = int(snbr.strip())
                # get the number of observation records stored in the next line
                next_line = next(fh)
                if next_line.startswith('POINT_NUM'):
                    _, n_records = next_line.split()
                    n_records = int(n_records)
                else:
                    raise ValueError('Unexpected line in TRACK output file.')

                # create a new observations list
                storm_obs = []

                """ Read in the storm's observations """
                # For each observation record
                for _, obs_line in zip(list(range(n_records)), fh):

                    # get each observation element
                    split_line = obs_line.strip().split('&')

                    # get observation date, T63 lat & lon, and vort
                    # (in case higher resolution data are not available)
                    date, lon, lat, vort = split_line[0].split()
                    date = parse_date(date, calendar)

                    # get full resolution 850 hPa maximum vorticity (s-1)
                    vort = float(vort)
                    # vort = float(storm_centre_record[3])
                    # vort = float(split_line[::-1][nlevels_T63+2+ex_cols])
                    # vort_idx = (nlevels_T63+2)*3
                    # vort = float(split_line[vort_idx])

                    # get storm location of maximum vorticity (full resolution field)
                    # lat = float(split_line[::-1][8+ex_cols])
                    # lon = float(split_line[::-1][9+ex_cols])
                    # storm_centre_record = split_line[0].split(' ')
                    lat = float(lat)
                    lon = float(lon)

                    # if higher resolution lat/lon data is not available then use lat
                    # lon from T42 resolution data
                    # if lat == 1e12 or lon == 1e12 or lat == 1.0e25 or lon == 1.0e25:
                    #    lat = float(tmp_lat)
                    #    lon = float(tmp_lon)

                    # get T63 vorticity
                    T63_1 = float(split_line[1 * 3])
                    T63_2 = float(split_line[2 * 3])
                    T63_3 = float(split_line[3 * 3])
                    T63_4 = float(split_line[4 * 3])
                    T63_5 = float(split_line[5 * 3])
                    T63_6 = float(split_line[6 * 3])
                    T63_7 = float(split_line[7 * 3])
                    T63_diff = (T63_1 - T63_7)

                    # get full resolution mslp
                    mslp = float(split_line[8 * 3])
                    if mslp > 1.0e4:
                        mslp /= 100
                    mslp = float(round(mslp, 1))

                    # get full resolution 925 hPa maximum wind speed (m/s)
                    vmax = float(split_line[9 * 3])

                    # check for mslp-vmax mix-up
                    if mslp < 500. and vmax > 500.:
                        mslp, vmax = vmax, mslp

                    # store vmax in knots (1 m/s = 1.944 kts) to match observations
                    vmax_kts = vmax * 1.944

                    # get 10m wind speed
                    v10m = float(split_line[10 * 3])
                    v10m_lat = float(split_line[(10 * 3) - 1])
                    v10m_lon = float(split_line[(10 * 3) - 2])

                    # Hart parameters
                    TL = float(split_line[::-1][3])
                    TU = float(split_line[::-1][2])
                    B = float(split_line[::-1][1])

                    # store observations
                    storm_obs.append(storm_assess.Observation(date, lat, lon, vort, vmax, mslp,
                                                              extras={'vmax_kts': vmax_kts, 'v10m': v10m,
                                                                      'v10m_lat': v10m_lat, 'v10m_lon': v10m_lon,
                                                                      'TL': TL, 'TU': TU, 'B': B}))

                # Yield storm
                yield storm_assess.Storm(snbr, storm_obs, extras={})


def load_hurdat2(fh, ex_cols=0, calendar=None):
    """
    ADAPTED TO READ REFORMATTED HURDAT2 TRACKS.

    Load function to read in Reading Universities TRACK output.
    Should work for individual ensemble member files and also the
    'combine' file.

    Note: All files need to contain the DATES of the storms, not the
    original timestep as output by TRACK.

    Note: Any fields in addition to the full field vorticity, mslp
    and 925 hPa max winds (e.g. 10m winds or precipitation) are not
    currently stored. If you want these values you need to read in
    the data file and include the variables in the 'extras' dictionary.

    """
    # allow users to pass a filename instead of a file handle.
    if isinstance(fh, str):
        with open(fh, 'r') as fh:
            for data in load(fh, ex_cols=ex_cols, calendar=calendar):
                yield data

    else:
        # for each line in the file handle
        for line in fh:
            if line.startswith('TRACK_NUM'):
                split_line = line.split()
                if split_line[2] == 'ADD_FLD':
                    number_fields = int(split_line[3])
                else:
                    raise ValueError('Unexpected line in TRACK output file.')

            if line.startswith('TRACK_ID'):
                # This is a new storm. Store the storm number.
                try:
                    _, snbr, _, _ = line.split()
                except:
                    _, snbr = line.split()
                snbr = int(snbr.strip())

                # Now get the number of observation records stored in the next line
                next_line = next(fh)
                if next_line.startswith('POINT_NUM'):
                    _, n_records = next_line.split()
                    n_records = int(n_records)
                else:
                    raise ValueError('Unexpected line in TRACK output file.')

                # Create a new observations list
                storm_obs = []

                """ Read in the storm's observations """
                # For each observation record
                for _, obs_line in zip(list(range(n_records)), fh):

                    # Get each observation element
                    split_line = obs_line.strip().split('&')

                    # Get observation date and T42 lat lon location in case higher
                    # resolution data are not available
                    date, tmp_lon, tmp_lat, _ = split_line[0].split()

                    date = parse_date(date, calendar)

                    # Get full resolution mslp
                    mslp = split_line[::-1][4 + ex_cols]
                    mslp = float(mslp)
                    if mslp > 1.0e4:
                        mslp /= 100
                    mslp = float(round(mslp, 1))

                    # Get full resolution 925hPa maximum wind speed (m/s)
                    vmax = float(split_line[::-1][7 + ex_cols])

                    # Also store vmax in knots (1 m/s = 1.944 kts) to match observations
                    vmax_kts = vmax * 1.944

                    # Get full resolution 850 hPa maximum vorticity (s-1)
                    vort = 0.

                    # Get storm location of maximum vorticity (full resolution field)
                    storm_centre_record = split_line[0].split(' ')
                    lat = float(storm_centre_record[2])
                    lon = float(storm_centre_record[1])

                    # Get 10m wind speed
                    v10m = float(split_line[::-1][7 + ex_cols])

                    # Store observations
                    storm_obs.append(storm_assess.Observation(date, lat, lon, vort, vmax, mslp,
                                                              extras={'vmax_kts': vmax_kts, 'v10m': v10m}))

                # Yield storm
                yield storm_assess.Storm(snbr, storm_obs, extras={})


def write(storms, file_name):
    """Write the storms to a text file using the TRACK layout

    Args:
        storms (list of storm_assess.Storm): Storm objects as loaded in by :func:`load`
        file_name (str):
    """
    tr_count = len(storms)
    extras = storms[-1].obs[0].extras.keys()
    number_fields = 2 + len(extras)
    with open(file_name, "w") as file_object:
        # Write the file header
        file_object.write("0\n")
        # The lines in the middle of the "0" and "0 0" can contain any extra information
        # so I'm adding in the names of variables
        file_object.write(f"ADDED_FIELDS: vmax MSLP {' '.join(extras)}\n")
        file_object.write("0 0\n")
        file_object.write(f"TRACK_NUM {tr_count} ADD_FLD {number_fields} {number_fields} &{'0' * number_fields}\n")
        for i, storm in enumerate(storms):
            # Write the storm header
            date = storm.genesis_date().strftime("%Y%m%d%H")
            num = storm.nrecords()
            file_object.write(f"TRACK_ID {storm.snbr} START_TIME {date}\n")
            file_object.write(f"POINT_NUM  {num}\n")
            # Write each line of observations for the storm
            for ob in storm.obs:
                date = ob.date.strftime("%Y%m%d%H")
                line_to_write = f"{date} {ob.lon} {ob.lat} {ob.vort} & {ob.vmax} & {ob.mslp} & "
                if number_fields > 2:
                    line_to_write += " &".join([str(ob.extras[key]) for key in extras])
                    line_to_write += " & "
                file_object.write(line_to_write + "\n")


def parse_date(date, calendar=None):
    if len(date) == 10:  # i.e., YYYYMMDDHH
        if calendar == "netcdftime":
            yr = int(date[0:4])
            mn = int(date[4:6])
            dy = int(date[6:8])
            hr = int(date[8:10])
            return cftime.datetime(yr, mn, dy, hour=hr, calendar="360_day")
        else:
            return datetime.datetime.strptime(date.strip(), "%Y%m%d%H")
    else:
        return int(date)
