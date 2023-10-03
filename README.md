## Install
A simple pip install fails because the cartopy package doesn't install through pip. To fix this, it is easiest to install this package in a conda environment that already has iris installed, e.g. running
> conda install -c conda-forge iris

first because iris is a required package and has cartopy as a dependency, so will handle those issues.

Then install this package
> pip install .