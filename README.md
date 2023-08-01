## Install
A simple pip install fails for two reasons:
1. The cartopy package doesn't install through pip
2. The netcdftime package uses Cython but doesn't install it as a dependency

To fix the first issue it is easiest to install this package in a conda environment that
already has iris installed, e.g. running
> conda install -c conda-forge iris

first because iris is a required package and has cartopy as a dependency, so will handle
those issues.  To fix the second issue, install Cython first with
> pip install Cython

Then install this package
> pip install .