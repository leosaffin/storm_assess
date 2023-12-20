import pytest
import numpy as np

from storm_assess.functions import xarray_functions


@pytest.mark.parametrize("testdata", ["storms_xarray", "storms_xarray_datetime64"])
@pytest.mark.parametrize(
    "year,months,nstorms",
    [
        (2000, [1], 0),
        (2000, [5], 3),
        (2000, list(range(1, 12+1)), 46),
        (2000, list(range(7, 12+1)) + list(range(1, 7)), 53),
    ]
)
def test_storms_in_time_range(testdata, request, year, months, nstorms):
    testdata = request.getfixturevalue(testdata)
    storms = list(xarray_functions._storms_in_time_range(testdata, year, months))
    assert len(storms) == nstorms


@pytest.mark.parametrize(
    "basin,expected",
    [
        ("na", True),
        ("ep", False),
    ]
)
def test_storm_in_basin(storms_xarray, basin, expected):
    result = xarray_functions.storm_in_basin(storms_xarray[0], basin)
    assert result is expected


@pytest.mark.parametrize("testdata", ["storms_xarray", "storms_xarray_datetime64"])
def test_get_genesis_months(testdata, request):
    storms = request.getfixturevalue(testdata)
    genesis_months = xarray_functions._get_genesis_months(
        storms, [2000], "na",
    )
    assert (genesis_months == np.array([5, 5, 7, 7, 8, 8, 8, 9, 9, 10, 10, 10])).all()


@pytest.mark.parametrize("testdata", ["storms_xarray", "storms_xarray_datetime64"])
def test_get_monthly_storm_count(testdata, request):
    storms = request.getfixturevalue(testdata)
    count = xarray_functions._get_monthly_storm_count(
        storms, [2000], [5, 6, 7, 8, 9, 10, 11], "na",
    )
    assert (count == np.array([2, 0, 2, 3, 2, 3, 0])).all()


def test_storm_lats_lons(storms_xarray):
    lats, lons, count = xarray_functions.storm_lats_lons(
        storms_xarray, [2000], [5, 6, 7, 8, 9, 10, 11], "na", lysis=True
    )
    assert lats == pytest.approx(np.array([
        59.985115, 12.70198, 34.871532, 18.358423, 12.733725, 13.225278, 48.884293,
        9.66584,  71.427483, 38.987488, 15.821413, 27.152714,
    ]))
    assert lons == pytest.approx(np.array([
        327.931183, 262.675873, 168.326675, 237.941574, 234.826981, 265.319946,
        319.550232, 329.237762, 33.47929, 345.130951, 255.699951, 287.326996
    ]))
    assert count == 12


def test_get_projected_track():
    pass
