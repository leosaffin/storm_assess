import cftime

import storm_assess
from storm_assess import track


def test_load():
    storms = list(
        track.load(storm_assess.SAMPLE_TRACK_DATA, ex_cols=3, calendar="netcdftime")
    )

    assert len(storms) == 540

    # Check first and last points are correct
    assert len(storms[0]) == 85
    assert storms[0].obs[0].date == cftime.datetime(2000, 5, 6, calendar="360_day")
    assert storms[0].obs[0].lon == 285.375793
    assert storms[0].obs[0].lat == 22.385307
    assert storms[0].obs[0].vort == 1.569625e00
    assert storms[0].obs[0].mslp == float(round(1.016622e05 / 100, 1))

    assert len(storms[-1]) == 30
    assert storms[-1].obs[-1].date == cftime.datetime(2011, 11, 21, 6, calendar="360_day")
    assert storms[-1].obs[-1].lon == 276.124756
    assert storms[-1].obs[-1].lat == 12.903164
    assert storms[-1].obs[-1].vort == 3.415816e00
    assert storms[-1].obs[-1].mslp == float(round(1.008229e05 / 100, 1))


def test_load_no_assumptions():
    storms = list(
        track.load_no_assumptions(storm_assess.SAMPLE_TRACK_DATA, calendar="netcdftime")
    )

    assert len(storms) == 540

    # Check first and last points are correct
    assert storms[0].attrs["track_id"] == 1
    assert storms[0].attrs["start_time"] == storms[0].time[0].data[()]
    assert len(storms[0].time) == 85
    assert storms[0].time[0] == cftime.datetime(2000, 5, 6, calendar="360_day")
    assert storms[0].longitude[0] == 285.375793
    assert storms[0].latitude[0] == 22.385307
    assert storms[0].vorticity[0] == 1.569625e00
    # assert storms[0].obs[0].mslp == 1.016622e+05 / 100, 1)

    assert storms[-1].attrs["track_id"] == 30
    assert storms[-1].attrs["start_time"] == storms[-1].time[0].data[()]
    assert len(storms[-1].time) == 30
    assert storms[-1].time[-1] == cftime.datetime(2011, 11, 21, 6, calendar="360_day")
    assert storms[-1].longitude[-1] == 276.124756
    assert storms[-1].latitude[-1] == 12.903164
    assert storms[-1].vorticity[-1] == 3.415816e00
    # assert storms[-1].obs[-1].mslp == 1.008229e+05 / 100, 1)
