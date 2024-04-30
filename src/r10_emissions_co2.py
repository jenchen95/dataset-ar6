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
    )
    if co2_var[pairs[3]] == 'ccs_ele':
        if df_interp.filter(pl.col('year') == 2025)['value'].to_numpy() <= 0.05:
            ccs_baseyear = df_interp.filter(pl.col('year') == 2020)['value'].to_numpy()
            df_interp = df_interp.with_columns(
                value=pl.when(pl.col('year').is_between(2021, 2024)).then(ccs_baseyear).otherwise(pl.col('value'))
            )

    co2_interp.append(df_interp)
co2 = pl.concat(co2_interp)

# Calculate bau electricity emissions
ele_gen = pl.read_parquet('../data/data_task/r10_supply_ele.parquet')
ele_co2 = (  # join and calculate intensity
    ele_gen.rename({'value':'supply'})
    .join(
        co2.filter(pl.col('scope') == 'ele').rename({'value':'co2'}), on=['model','scenario','region','year'], how='inner'  # TODO: maybe future fillnull for model-scenario doesn't have ele emissions or ele supply
    ).with_columns(
        intensity=pl.col('co2') / pl.col('supply')  # Mt/EJ
    )
)

ele_co2_bau = []
for pairs, df in ele_co2.group_by(['model','scenario','region']):
    print('Setting bau intensity for ele_gen:', pairs)
    intensity_const = df.filter(pl.col('year') == 2020)['intensity'].to_numpy()
    df = df.with_columns(
        intensity=pl.when(pl.col('year') >= 2021)
        .then(intensity_const)
        .otherwise(pl.col('intensity'))
    ).with_columns(
        value=pl.col('supply') * pl.col('intensity')
    ).with_columns(
        scope=pl.lit('ele_bau')
    ).select(pl.exclude('unit')).rename({'unit_right':'unit'})

    ele_co2_bau.append(df)

ele_gen_bau = pl.concat(ele_co2_bau)
co2 = co2.vstack(
    ele_gen_bau.select(pl.col('model','scenario','region','scope','unit','year','value'))
    )
# Exporting
co2.write_parquet('../data/data_task/r10_emissions_co2.parquet')
print('co2 done')
