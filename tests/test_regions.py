import numpy as np

from storm_assess import regions


def test_landfall_europe(storms_xarray):
    result = [regions.landfall_europe(storm).any() for storm in storms_xarray]
    assert np.count_nonzero(result) == 53
