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

import datetime
import netcdftime

import storm_assess


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
                    
                    if len(date) == 10: # i.e., YYYYMMDDHH
                        if calendar == 'netcdftime':
                            yr = int(date[0:4])
                            mn = int(date[4:6])
                            dy = int(date[6:8])
                            hr = int(date[8:10])
                            date = netcdftime.datetime(yr, mn, dy, hour=hr)
                        else:
                            date = datetime.datetime.strptime(date.strip(), '%Y%m%d%H')
                    else:
                        date = int(date)

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


if __name__ == '__main__':
    fname = storm_assess.SAMPLE_TRACK_DATA
    print('Loading TRACK data from file:' , fname)    
    storms = list(load(fname, ex_cols=3, calendar='netcdftime'))
    print('Number of model storms: ', len(storms))
    
    # Print storm details:
    for storm in storms: 
        #print storm.snbr, storm.genesis_date()
        for ob in storm.obs:
            print(ob.date, ob.lon, ob.lat, ob.vmax, ob.extras['vmax_kts'], ob.mslp, ob.vort)
    print('Number of model storms: ', len(storms)) 

