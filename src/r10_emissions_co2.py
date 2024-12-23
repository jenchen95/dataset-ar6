import polars as pl 
import numpy as np
from func import fit_spline
# Reading and Filtering
co2_var = {
    'Emissions|CO2': 'total', 
    'Emissions|CO2|Energy|Supply|Electricity': 'ele', 
    'Emissions|CO2|Energy|Demand|Transportation': 'trp', 
    'Carbon Sequestration|CCS|Fossil|Energy|Supply|Electricity': 'ccs_ele',
    'Carbon Sequestration|CCS|Biomass|Energy|Supply|Electricity': 'beccs_ele',
    'Carbon Sequestration|CCS|Biomass|Energy|Supply': 'beccs_energy_supply',
    'Carbon Sequestration|Direct Air Capture': 'dac'}

co2_interp_min_is_zero = {
    'Emissions|CO2': False,
    'Emissions|CO2|Energy|Supply|Electricity': False,
    'Emissions|CO2|Energy|Demand|Transportation': True,
    'Carbon Sequestration|CCS|Fossil|Energy|Supply|Electricity': True,
    'Carbon Sequestration|CCS|Biomass|Energy|Supply|Electricity': True,
    'Carbon Sequestration|CCS|Biomass|Energy|Supply': True,
    'Carbon Sequestration|Direct Air Capture': True}

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
                'value': fit_spline(df['value'], df['year'], min_is_zero=co2_interp_min_is_zero[pairs[3]]),
            }
        )
    )
    if co2_var[pairs[3]] in ['ccs_ele', 'ccs_fossil_energy_supply', 'ccs_fossil', 'beccs_ele', 'beccs_energy_supply', 'dac']:
        if df_interp.filter(pl.col('year') == 2025)['value'].to_numpy() <= 0.05:
            ccs_baseyear = df_interp.filter(pl.col('year') == pl.min('year'))['value'].to_numpy()
            df_interp = df_interp.with_columns(
                value=pl.when(pl.col('year').is_between(pl.min('year')+1, 2024)).then(ccs_baseyear).otherwise(pl.col('value'))
            )

    co2_interp.append(df_interp)
co2 = pl.concat(co2_interp)
# Fill null with 0 for ccs_ele, beccs_ele, beccs_energy_supply, dac
co2 = (
    co2.pivot(index=['model','scenario','region','year','unit'],columns=['scope'],values='value')
    .with_columns(
        ccs_ele=pl.when(pl.col('ccs_ele').is_not_null())
        .then(pl.col('ccs_ele'))
        .otherwise(pl.lit(0)),
        beccs_ele=pl.when(pl.col('beccs_ele').is_not_null())
        .then(pl.col('beccs_ele'))
        .otherwise(pl.lit(0)),
        beccs_energy_supply=pl.when(pl.col('beccs_energy_supply').is_not_null())
        .then(pl.col('beccs_energy_supply'))
        .otherwise(pl.lit(0)),
        dac=pl.when(pl.col('dac').is_not_null())
        .then(pl.col('dac'))
        .otherwise(pl.lit(0))
    )
    .melt(id_vars=['model','scenario','region','year','unit'], variable_name='scope', value_name='value')
    .select(pl.col('model','scenario','region','scope','unit','year','value'))
)

# Calculate bau electricity emissions
ele_gen = pl.read_parquet('../data/data_task/r10_supply_ele.parquet')
ele_co2 = (  # join and calculate intensity
    ele_gen
    .filter(pl.col('variable') == 'Secondary Energy|Electricity')
    .rename({'value':'supply'})
    .join(
        co2.filter(pl.col('scope') == 'ele').rename({'value':'co2'}), on=['model','scenario','region','year'], how='inner'  # TODO: maybe future fillnull for model-scenario doesn't have ele emissions or ele supply
    )
    .with_columns(
        intensity=pl.col('co2') / pl.col('supply')  # Mt/EJ
    )
    .drop_nulls(subset=['intensity'])
)

ele_co2_bau = []
for pairs, df in ele_co2.group_by(['model','scenario','region']):
    # Find the year of maximum intensity
    max_intensity_row = df.filter(pl.col('intensity') == pl.col('intensity').max())
    intensity_const = max_intensity_row['intensity'].to_numpy() + 1e-1  # add a small number to avoid division by zero finally
    max_intensity_year = max_intensity_row['year'].to_numpy()[0]
    
    df = df.with_columns(
        intensity=pl.when(pl.col('year') >= max_intensity_year)
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

# Calculate bau transportation emissions
vehicle_stock = pl.read_parquet('../data/data_import/gdp_vehicle_fitting.parquet')
trp_co2 = (  # join and calculate intensity
    vehicle_stock
    .join(
        co2.filter(pl.col('scope') == 'trp').rename({'value':'co2'}), on=['model','scenario','region','year'], how='inner'  # TODO: maybe future fillnull for model-scenario in co2 doesn't have vehicle stock. Slighly different from ele_gen, vehicle stock contains all model-scenario used in this project (474 model-scenario)
    ).with_columns(
        intensity=pl.col('co2') / pl.col('vehicle') * 10e6 # Mt/million vehicle or t/vehicle
    )
)

trp_co2_bau = []
for pairs, df in trp_co2.group_by(['model','scenario','region']):
    print('Setting bau intensity for trp:', pairs)
    intensity_const = df.filter(pl.col('year') == pl.min('year'))['intensity'].to_numpy()
    df = (
        df.with_columns(
        intensity=pl.when(pl.col('year') >= pl.min('year')+1)
        .then(intensity_const)
        .otherwise(pl.col('intensity'))
        ).with_columns(
        value=pl.col('vehicle') * pl.col('intensity') / 10e6  # Mt
        ).with_columns(
        scope=pl.lit('trp_bau')
        ).select(pl.exclude('unit')).rename({'unit_right':'unit'})
    )

    trp_co2_bau.append(df)

vehicle_stock_bau = pl.concat(trp_co2_bau)
co2 = co2.vstack(
    vehicle_stock_bau.select(pl.col('model','scenario','region','scope','unit','year','value'))
    )


# Exporting
co2.write_parquet('../data/data_task/r10_emissions_co2.parquet')
print('co2 done')
