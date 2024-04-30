import polars as pl 
import numpy as np
from func import fit_spline
# Reading and Filtering
co2_var = {
    'Emissions|CO2': 'total', 
    'Emissions|CO2|Energy|Supply|Electricity': 'ele', 
    'Emissions|CO2|Energy|Demand|Transportation': 'trp', 
    'Carbon Sequestration|CCS|Fossil|Energy|Supply|Electricity': 'ccs_ele'}

co2 = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(co2_var.keys()))
    .select(pl.exclude('category'))
    .sort('model','scenario','region','year')
    .collect()
)
unit_co2 = co2['unit'][0]

co2_interp = []
for pairs, df in co2.group_by(['model','scenario','region','variable']):
    print('Interpolating for co2:', pairs)
    year = np.arange(df['year'].min(), df['year'].max() + 1)
    df_interp = (
        pl.DataFrame(
            {
                'model': pairs[0],
                'scenario': pairs[1],
                'region': pairs[2],
                'scope': co2_var[pairs[3]],
                'unit': unit_co2,
                'year': year,
                'value': fit_spline(df['value'], df['year']),
            }
        )
        .with_columns(abatement_yearly=pl.col('value').diff().neg())
        .with_columns(abatement_baseyear=pl.col('abatement_yearly').cumsum())  # for ccs techs, these two columns may be meaningless
    )
    if co2_var[pairs[3]] == 'ccs_ele':
        if df_interp.filter(pl.col('year') == 2025)['value'].to_numpy() <= 0.05:
            ccs_baseyear = df_interp.filter(pl.col('year') == 2020)['value'].to_numpy()
            df_interp = df_interp.with_columns(
                value=pl.when(pl.col('year').is_between(2021, 2024)).then(ccs_baseyear).otherwise(pl.col('value'))
            )

    co2_interp.append(df_interp)
co2 = pl.concat(co2_interp)

# Exporting
co2.write_parquet('../data/data_task/r10_emissions_co2.parquet')
print('co2 done')
