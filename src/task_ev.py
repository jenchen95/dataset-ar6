# Import
import polars as pl
import numpy as np
from scipy.interpolate import CubicSpline

# Interpolate historical data for gdp_per_capita
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

# Reading
gdp_pop = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(['GDP|MER', 'Population']))
    .collect()
    .pivot(index=['category','model','scenario','region','year'], columns='variable', values='value')
    .drop_nulls(subset=['GDP|MER','Population'])
    .with_columns(gdp=pl.col('GDP|MER') * 1e9)  # billion dollars to dollar
    .with_columns(population=pl.col('Population') * 1e6)  # million people to people
    .with_columns(unit=pl.lit('GDP (constant 2010 US$)'))
    .select(pl.exclude('GDP|MER','Population'))
)

trp_ele = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(['Final Energy|Transportation|Electricity']))
    .collect()
    .pivot(index=['category','model','scenario','region','year'], columns='variable', values='value')
    .drop_nulls(subset=['Final Energy|Transportation|Electricity'])
    .with_columns(trp_ele=pl.col('Final Energy|Transportation|Electricity'))
    .with_columns(unit=pl.lit('EJ'))
    .select(pl.exclude('Final Energy|Transportation|Electricity'))
)

# Interpolate
gdp_per_capita = []
r10_list = pl.read_csv('../data/data_man/r10_list.csv')

for i,j in gdp_pop.select(pl.col('model','scenario')).unique().rows():
    for k in r10_list['region'].to_list():
        print(f'Now is {i}-{j}-{k}')
        region = (
            gdp_pop.filter((pl.col('model') == i) & (pl.col('scenario') ==j) & (pl.col('region') == k))
            .select(pl.col('region','year','gdp','population'))
            .sort('year')
        )
        if region.is_empty():
            continue
        years = region['year']
        gdp = region['gdp']
        population = region['population']

        gdp = fit_spline(gdp, years, parse=False)
        population = fit_spline(population, years , parse=False).astype(int)  # integer population

        gdp_per_capita.append(
            pl.DataFrame({'year': np.arange(years[0], years[-1]+1),
                          'gdp': gdp,
                          'population': population})
            .with_columns(pl.lit(i).alias('model'),
                          pl.lit(j).alias('scenario'),
                          pl.lit(k).alias('region'),
                          gdp_per_cap = pl.col('gdp') / pl.col('population'),
                          unit = pl.lit('US$(2010)'),
                          unit_pop = pl.lit('person')
                          )
        )

gdp_per_capita = (
    pl.concat(gdp_per_capita)
    .select(pl.col('model','scenario','region','year','gdp_per_cap','unit','population','unit_pop'))
)

trp_ele_interp = []

for i,j in trp_ele.select(pl.col('model','scenario')).unique().rows():
    for k in r10_list['region'].to_list():
        print('Now interpolating Final Energy|Transportation|Electricity')
        print(f'Now is {i}-{j}-{k}')
        region = (
            trp_ele.filter((pl.col('model') == i) & (pl.col('scenario') ==j) & (pl.col('region') == k))
            .select(pl.col('region','year','trp_ele'))
            .sort('year')
        )
        if region.is_empty():
            continue
        years = region['year']
        ele = region['trp_ele']

        ele = fit_spline(ele, years, parse=False)

        trp_ele_interp.append(
            pl.DataFrame({'year': np.arange(years[0], years[-1]+1),
                          'trp_ele': ele})
            .with_columns(pl.lit(i).alias('model'),
                          pl.lit(j).alias('scenario'),
                          pl.lit(k).alias('region'),
                          unit = pl.lit('EJ'),
                          )
        )

trp_ele_interp = (
    pl.concat(trp_ele_interp)
    .select(pl.col('model','scenario','region','year','trp_ele','unit'))
)

# Export
gdp_per_capita.write_parquet('../data/data_task/gdp_per_cap_future.parquet')
print('GDP per capita data exported')

trp_ele_interp.write_parquet('../data/data_task/trp_ele_future.parquet')
print('Final Energy|Transportation|Electricity data exported')
