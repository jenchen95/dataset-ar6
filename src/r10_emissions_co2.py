import polars as pl 
import numpy as np
from scipy.interpolate import CubicSpline

# Fit spline
def fit_spline(series, years, parse=False):
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

    spline = CubicSpline(fit_years, fit_values)
    # Predict for all years using the fitted spline interpolation
    granu_values = spline(granu_years)

    # Ensure all values are non-negative
    granu_values = np.maximum(granu_values, 0)
    granu_values = granu_values.ravel()  # Convert to 1D array

    # pad with 0s if the first year is not the beginning year
    granu_values = np.pad(granu_values, (granu_years[0] - years[0], 0), 'constant', constant_values=0)

    return granu_values

# Reading and Filtering
co2_var = ['Emissions|CO2']
co2_ele_var = ['Emissions|CO2|Energy|Supply|Electricity']
co2_trp_var = ['Emissions|CO2|Energy|Demand|Transportation']

co2 = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(co2_var))
    .select(pl.exclude('category'))
    .filter(pl.col('year').is_between(2020, 2060))
    .sort('model','scenario','region','year')
    .collect()
)

co2_interp = []
for pairs, df in co2.group_by(['model','scenario','region']):
    print('Interpolating for co2:', pairs)
    co2_interp.append(
        pl.DataFrame(
            {
                'model': pairs[0],
                'scenario': pairs[1],
                'region': pairs[2],
                'unit': 'Mt CO2/yr',
                'year': np.arange(2020, 2061),
                'value': fit_spline(df['value'], df['year']),
            }
        )
        .with_columns(abatement_yearly=pl.col('value').diff().neg())
        .with_columns(abatement_baseyear=pl.col('abatement_yearly').cumsum())
    )
co2 = pl.concat(co2_interp)

co2_ele = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(co2_ele_var))
    .select(pl.exclude('category'))
    .filter(pl.col('year').is_between(2020, 2060))
    .sort('model','scenario','region','year')
    .collect()
)

co2_ele_interp = []
for pairs, df in co2_ele.group_by(['model','scenario','region']):
    print('Interpolating for co2_ele:', pairs)
    co2_ele_interp.append(
        pl.DataFrame(
            {
                'model': pairs[0],
                'scenario': pairs[1],
                'region': pairs[2],
                'unit': 'Mt CO2/yr',
                'year': np.arange(2020, 2061),
                'value': fit_spline(df['value'], df['year']),
            }
        )
        .with_columns(abatement_yearly=pl.col('value').diff().neg())
        .with_columns(abatement_baseyear=pl.col('abatement_yearly').cumsum())
    )
co2_ele = pl.concat(co2_ele_interp)

co2_trp = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(co2_trp_var))
    .select(pl.exclude('category'))
    .filter(pl.col('year').is_between(2020, 2060))
    .sort('model','scenario','region','year')
    .collect()
)

co2_trp_interp = []
for pairs, df in co2_trp.group_by(['model','scenario','region']):
    print('Interpolating for co2_trp:', pairs)
    co2_trp_interp.append(
        pl.DataFrame(
            {
                'model': pairs[0],
                'scenario': pairs[1],
                'region': pairs[2],
                'unit': 'Mt CO2/yr',
                'year': np.arange(2020, 2061),
                'value': fit_spline(df['value'], df['year']),
            }
        )
        .with_columns(abatement_yearly=pl.col('value').diff().neg())
        .with_columns(abatement_baseyear=pl.col('abatement_yearly').cumsum())
    )
co2_trp = pl.concat(co2_trp_interp)

# Exporting
co2.write_parquet('../data/data_task/r10_emissions_co2.parquet')
print('co2 done')
co2_ele.write_parquet('../data/data_task/r10_emissions_co2_ele.parquet')
print('co2_ele done')
co2_trp.write_parquet('../data/data_task/r10_emissions_co2_trp.parquet')
print('co2_trp done')
