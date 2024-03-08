import pathlib

import cftime
import pytest

import storm_assess
from storm_assess import track


@pytest.mark.parametrize("load_function", [track.load, track.load_hurdat2])
def test_load(load_function):
    storms = list(
        load_function(storm_assess.SAMPLE_TRACK_DATA, ex_cols=3, calendar="netcdftime")
    )

    assert len(storms) == 540

    # Check first and last points are correct
    assert len(storms[0]) == 85
    assert storms[0].obs[0].date == cftime.datetime(2000, 5, 6, calendar="360_day")
    assert storms[0].obs[0].lon == 285.375793
    assert storms[0].obs[0].lat == 22.385307
    assert storms[0].obs[0].vort == 1.569625e00
    assert storms[0].obs[0].vmax == 9.860207e+00
    assert storms[0].obs[0].mslp == float(round(1.016622e05 / 100, 1))

    assert len(storms[-1]) == 30
    assert storms[-1].obs[-1].date == cftime.datetime(2011, 11, 21, 6, calendar="360_day")
    assert storms[-1].obs[-1].lon == 276.124756
    assert storms[-1].obs[-1].lat == 12.903164
    assert storms[-1].obs[-1].vort == 3.415816e00
    assert storms[-1].obs[-1].vmax == 1.000000e+12
    assert storms[-1].obs[-1].mslp == float(round(1.008229e05 / 100, 1))


def test_load_no_assumptions(storms_xarray):
    assert len(storms_xarray) == 540

    # Check first and last points are correct
    assert storms_xarray[0].attrs["track_id"] == 1
    assert storms_xarray[0].attrs["start_time"] == storms_xarray[0].time[0].data[()]
    assert len(storms_xarray[0].time) == 85
    assert storms_xarray[0].time[0] == cftime.datetime(2000, 5, 6, calendar="360_day")
    assert storms_xarray[0].longitude[0] == 285.375793
    assert storms_xarray[0].latitude[0] == 22.385307
    assert storms_xarray[0].vorticity[0] == 1.569625e00
    assert storms_xarray[0].vmax[0] == 1.385634e+01
    assert storms_xarray[0].mslp[0] == 1.016622e+05
    assert storms_xarray[0].v10m[0] == 9.860207e+00

    assert storms_xarray[-1].attrs["track_id"] == 30
    assert storms_xarray[-1].attrs["start_time"] == storms_xarray[-1].time[0].data[()]
    assert len(storms_xarray[-1].time) == 30
    assert storms_xarray[-1].time[-1] == cftime.datetime(2011, 11, 21, 6, calendar="360_day")
    assert storms_xarray[-1].longitude[-1] == 276.124756
    assert storms_xarray[-1].latitude[-1] == 12.903164
    assert storms_xarray[-1].vorticity[-1] == 3.415816e00
    assert storms_xarray[-1].vmax[-1] == 8.001409e+00
    assert storms_xarray[-1].mslp[-1] == 1.008229e+05
    assert storms_xarray[-1].v10m[-1] == 1.000000e+12


def test_load_no_assumptions_storm():
    storms = track.load_no_assumptions(
        storm_assess.SAMPLE_TRACK_DATA, calendar="netcdftime", output_type="storm"
    )
    # Check first and last points are correct
    assert len(storms[0]) == 85
    assert storms[0].obs[0].date == cftime.datetime(2000, 5, 6, calendar="360_day")
    assert storms[0].obs[0].lon == 285.375793
    assert storms[0].obs[0].lat == 22.385307
    assert storms[0].obs[0].vort == 1.569625e00
    # assert storms[0].obs[0].vmax == 9.860207e+00
    # assert storms[0].obs[0].mslp == float(round(1.016622e05 / 100, 1))

    assert len(storms[-1]) == 30
    assert storms[-1].obs[-1].date == cftime.datetime(2011, 11, 21, 6, calendar="360_day")
    assert storms[-1].obs[-1].lon == 276.124756
    assert storms[-1].obs[-1].lat == 12.903164
    assert storms[-1].obs[-1].vort == 3.415816e00
    # assert storms[-1].obs[-1].vmax == 1.000000e+12
    # assert storms[-1].obs[-1].mslp == float(round(1.008229e05 / 100, 1))


def test_save_netcdf(storms_xarray):
    track.save_netcdf(storms_xarray, "test.nc")

    storms_copy = track.load_netcdf("test.nc")

    assert len(storms_xarray) == len(storms_copy)
    for n in range(len(storms_xarray)):
        for var in storms_xarray[n]:
            assert (storms_xarray[n][var].data == storms_copy[n][var].data).all()

    pathlib.Path("test.nc").unlink()
