import shapely
from haversine import haversine_vector
import numpy as np
from cartopy.io.shapereader import natural_earth
import geopandas

from storm_assess.functions.xarray_functions import get_projected_track


# Keep combined shapes rather than reproducing them multiple times
_cache = dict()


def _cached_shape(name):
    def decorator(function):
        def wrapper(*args, **kwargs):
            if name in _cache:
                return _cache[name]
            else:
                result = function(*args, **kwargs)
                _cache[name] = result
                return result
        return wrapper
    return decorator


def hits_europe(storm):
    """Check if a track overlaps Europe land using country outlines

    Args:
        storm (xarray.Dataset):

    Returns:
        bool: True if storm intersects Europe, False otherwise
    """
    europe = get_europe()
    linestring = get_projected_track(storm)

    return linestring.intersects(europe)


def landfall_europe(storm, distance=200):
    """Check whether the storm gets within a threshold distance of the europe coastline

    Args:
        storm (xarray.Dataset):
        distance (scalar, optional): Threshold distance for landfall in kilometres.
            Default is 200

    Returns:
        np.array:
            A boolean array matching the storm length (in time) saying which points are
            within the threshold distance.
    """
    europe_coast_xy = np.vstack([
        np.array(g.exterior.coords.xy).transpose() for g in get_europe().geoms
    ])
    europe_coast_yx = europe_coast_xy[:, ::-1]
    europe_coast_yx[:, 1] = (europe_coast_yx[:, 1] + 180) % 360 - 180

    storm_yx = np.array([storm.latitude.data, storm.longitude.data]).transpose()
    storm_yx[:, 1] = (storm_yx[:, 1] + 180) % 360 - 180

    # Array of distances between each storm point and all coastline points
    distances = haversine_vector(storm_yx, europe_coast_yx, comb=True)

    return (distances < distance).any(axis=0)


@_cached_shape(name="europe")
def get_europe():
    # Get filename of country boundaries from cartopy.
    # cartopy will download and keep the file if it has not been downloaded before
    fname = natural_earth(
        resolution='110m',
        category='cultural',
        name='admin_0_countries'
    )

    # Load into a pandas dataframe using geopandas
    df = geopandas.read_file(fname)

    # Select only European countries
    europe = df[df.CONTINENT == "Europe"]

    # Combine all country boundaries into a minimal set of boundaries
    # TODO - filter out colonies (French Guiana)
    geoms = []
    for n, row in europe.iterrows():
        if type(row.geometry) is shapely.geometry.polygon.Polygon:
            geoms.append(row.geometry)
        else:
            for g in row.geometry.geoms:
                geoms.append(g)
    europe_shape = shapely.union_all(geoms)

    return europe_shape
