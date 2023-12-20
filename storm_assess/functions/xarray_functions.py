"""
Provides example functions that are useful for assessing model
tropical storms.


"""
import numpy as np
import pandas
import cartopy.crs as ccrs
import shapely.geometry as sgeom

from storm_assess.functions import _get_time_range, _basin_polygon


def _fudge_time(time):
    return pandas.to_datetime(time.astype(str).data)


def _storms_in_time_range(storms, year, months):
    """Returns a generator of storms that formed during the desired time period """
    start_date, end_date = _get_time_range(year, months)

    for storm in storms:
        time_0 = _fudge_time(storm.time[0])

        if start_date <= time_0 < end_date:
            yield storm


def storm_in_basin(storm, basin):
    """ Returns True if a storm track intersects a defined ocean basin """
    rbox = _basin_polygon(basin)

    tr = sgeom.LineString(list(zip(storm.longitude, storm.latitude)))

    projected_track = ccrs.PlateCarree().project_geometry(tr, ccrs.Geodetic())

    return rbox.intersects(projected_track)


def _get_genesis_months(storms, years, basin):
    """
    Returns genesis month of all storms that formed within a
    given set of years

    """
    genesis_months = []
    for storm in storms:
        t0 = _fudge_time(storm.time[0])
        if t0.year in years and storm_in_basin(storm, basin):
            genesis_months.append(t0.month)
    return genesis_months


def _get_monthly_storm_count(storms, years, months, basin):
    """ Returns list of storm counts for a desired set of months """
    genesis_months = _get_genesis_months(storms, years, basin)
    monthly_count = []
    for month in months:
        monthly_count.append(genesis_months.count(month))
    return monthly_count


def storm_lats_lons(storms, years, months, basin, genesis=False,
                    lysis=False, max_intensity=False):
    """
    Returns array of latitude and longitude values for all storms that
    occurred within a desired year, month set and basin.

    To get genesis, lysis or max intensity results set:
    Genesis plot: genesis=True
    Lysis plot: lysis=True
    Maximum intensity (location of max wind):
    max_intensity=True

    """
    lats, lons = [], []
    count = 0
    for year in years:
        for storm in _storms_in_time_range(storms, year, months):
            if storm_in_basin(storm, basin):
                if genesis:
                    # print 'getting genesis locations'
                    lats.extend([storm.latitude[0]])
                    lons.extend([storm.longitude[0]])
                elif lysis:
                    # print 'getting lysis locations'
                    lats.extend([storm.latitude[-1]])
                    lons.extend([storm.longitude[-1]])
                elif max_intensity:
                    # print 'getting max int locations'
                    lats.extend([storm.obs_at_vmax().lat])
                    lons.extend([storm.obs_at_vmax().lon])
                else:
                    # print 'getting whole storm track locations'
                    lats.extend(storm.latitude.data)
                    lons.extend(storm.longitude.data)
                count += 1

    # Normalise lon values into the range 0-360
    lons = (np.array(lons) + 720) % 360

    return np.array(lats), lons, count


def get_projected_track(storm, map_proj=None):
    """ Returns track of storm as a linestring """
    track = sgeom.LineString(zip(storm.longitude, storm.latitude))

    if map_proj is None:
        return track
    else:
        return map_proj.project_geometry(track, ccrs.Geodetic())

