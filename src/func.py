import polars as pl
import numpy as np
from scipy.interpolate import CubicSpline

# Fit spline
def fit_spline(series, years, parse=False, min_is_zero=True, bc='natural'):
    """
    Applies cubic spline interpolation to each column of the DataFrame.
    """
    df = pl.DataFrame([years,series]).filter(pl.exclude('year').is_not_null())

    # Beginning year is the first year with non-null value
    granu_years = np.arange(df['year'].item(0), years.max() + 1)

    # If parse is True, parse df's years are divisible by 10
    if parse is True:
        df = df.filter(pl.col('year') % 10 == 0)

    fit_years = df['year'].to_numpy()
    fit_values = df.select(pl.exclude('year')).to_numpy()

    zero_ranges = []
    for i in range(len(fit_values) - 1):
        if fit_values[i] == 0 and fit_values[i + 1] == 0:
            zero_ranges.append((years[i], years[i + 1]))

    spline = CubicSpline(fit_years, fit_values, bc_type=bc)
    # Predict for all years using the fitted spline interpolation
    granu_values = spline(granu_years)

    for start, end in zero_ranges:
        granu_values[(granu_years >= start) & (granu_years <= end)] = 0

    # Ensure all values are non-negative
    if min_is_zero:
        granu_values = np.maximum(granu_values, 0)
    granu_values = granu_values.ravel()  # Convert to 1D array

    # pad with 0s if the first year is not the beginning year
    granu_values = np.pad(granu_values, (granu_years[0] - years[0], 0), 'constant', constant_values=0)

    return granu_values
