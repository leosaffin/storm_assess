import storm_assess
from storm_assess import track


def test_load():
    storms = list(track.load(
        storm_assess.SAMPLE_TRACK_DATA, ex_cols=3, calendar="netcdftime"
    ))

    assert len(storms) == 540

    # Check first and last points are correct
    assert len(storms[0]) == 85
    assert storms[0].obs[0].lon == 285.375793
    assert storms[0].obs[0].lat == 22.385307
    assert storms[0].obs[0].vort == 1.569625e+00
    assert storms[0].obs[0].mslp == float(round(1.016622e+05 / 100, 1))

    assert len(storms[-1]) == 30
    assert storms[-1].obs[-1].lon == 276.124756
    assert storms[-1].obs[-1].lat == 12.903164
    assert storms[-1].obs[-1].vort == 3.415816e+00
    assert storms[-1].obs[-1].mslp == float(round(1.008229e+05 / 100, 1))
