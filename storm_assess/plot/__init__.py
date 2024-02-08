import numpy as np
import matplotlib.pyplot as plt


def plot_track(tr, *args, ax=None, xname="lon", yname="lat", **kwargs):
    # Look for points along the track crossing the wrapping point in longitude
    # If any exist set the longitudes to wrap at the point at the opposite side of the
    # Earth (-180 degrees) from the current wrap point
    dlon = np.diff(tr[xname])
    if (np.abs(dlon) > 180).any():
        wrap_point = tr[xname].max().data[()]
        lon = ((tr[xname] + (wrap_point - 180)) % 360) - (wrap_point - 180)
    else:
        lon = tr[xname]

    if ax is None:
        ax = plt.gca()

    ax.plot(lon, tr[yname], *args, **kwargs)
