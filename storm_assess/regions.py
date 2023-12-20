import shapely
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
