import pytest

from storm_assess import SAMPLE_TRACK_DATA, track

# Names of added variables in test data for loading with no assumptions
_variable_names = [
    "vorticity_1",
    "vorticity_2",
    "vorticity_3",
    "vorticity_4",
    "vorticity_5",
    "vorticity_6",
    "vmax",
    "mslp",
    "v10m",
]


@pytest.fixture(scope="session")
def storms():
    return list(track.load(SAMPLE_TRACK_DATA, ex_cols=3, calendar="netcdftime"))


@pytest.fixture(scope="session")
def storms_xarray():
    return track.load_no_assumptions(
        SAMPLE_TRACK_DATA,
        calendar="netcdftime",
        variable_names=_variable_names,
    )


@pytest.fixture(scope="session")
def storms_xarray_datetime64():
    # Loading with calendar="netcdftime" forces the time variable to remain as a cftime
    # object, but time is otherwise converted to the distinctly inferior datetime64
    return track.load_no_assumptions(SAMPLE_TRACK_DATA, variable_names=_variable_names)
