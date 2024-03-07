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
"""

import storm_assess
from storm_assess.track import parse_date


# Load TRACK file
def load(fh, ex_cols=0, calendar=None):
    """
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
                    print(('using ex_cols = ',ex_cols))
                    nlevels_T63 = number_fields - 3
                
            # read storms
            if line.startswith('TRACK_ID'):
                # new storm: store the storm number
                try:
                    _, snbr, _, _ = line.split()
                except:
                    _, snbr = line.split()
                snbr =  int(snbr.strip())
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
                    #vort = float(storm_centre_record[3])
                    #vort = float(split_line[::-1][nlevels_T63+2+ex_cols])
                    #vort_idx = (nlevels_T63+2)*3
                    #vort = float(split_line[vort_idx])

                    # get storm location of maximum vorticity (full resolution field)
                    #lat = float(split_line[::-1][8+ex_cols])
                    #lon = float(split_line[::-1][9+ex_cols])
                    #storm_centre_record = split_line[0].split(' ')
                    lat = float(lat)
                    lon = float(lon)

                    # if higher resolution lat/lon data is not available then use lat 
                    # lon from T42 resolution data
                    #if lat == 1e12 or lon == 1e12 or lat == 1.0e25 or lon == 1.0e25:
                    #    lat = float(tmp_lat)
                    #    lon = float(tmp_lon)

                    # get T63 vorticity
                    T63_1 = float(split_line[1*3])
                    T63_2 = float(split_line[2*3])
                    T63_3 = float(split_line[3*3])
                    T63_4 = float(split_line[4*3])
                    T63_5 = float(split_line[5*3])
                    T63_6 = float(split_line[6*3])
                    T63_7 = float(split_line[7*3])
                    T63_diff = (T63_1 - T63_7)

                    # get full resolution mslp
                    mslp = float(split_line[8*3])
                    if mslp > 1.0e4:
                        mslp /= 100
                    mslp = float(round(mslp,1))
                    
                    # get full resolution 925 hPa maximum wind speed (m/s)
                    vmax = float(split_line[9*3])

                    # check for mslp-vmax mix-up
                    if mslp < 500. and vmax > 500.:
                        mslp,vmax = vmax,mslp

                    # store vmax in knots (1 m/s = 1.944 kts) to match observations
                    vmax_kts = vmax * 1.944
                    
                    # get 10m wind speed
                    v10m = float(split_line[10*3])
                    v10m_lat = float(split_line[(10*3)-1])
                    v10m_lon = float(split_line[(10*3)-2])
     
                    # Hart parameters
                    TL = float(split_line[::-1][3])
                    TU = float(split_line[::-1][2])
                    B = float(split_line[::-1][1])

                    # store observations
                    storm_obs.append(storm_assess.Observation(date, lat, lon, vort, vmax, mslp,
                        extras={'vmax_kts':vmax_kts,'v10m':v10m,'v10m_lat':v10m_lat,'v10m_lon':v10m_lon,'TL':TL,'TU':TU,'B':B}))
                    
                # Yield storm
                yield storm_assess.Storm(snbr, storm_obs, extras={})
