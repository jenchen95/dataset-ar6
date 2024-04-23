import polars as pl 

# Reading and Filtering
co2_var = ['Emissions|CO2']

co2 = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(co2_var))
    .select(pl.exclude('category'))
)

# Exporting
co2.sink_parquet('../data/data_task/r10_emissions_co2.parquet')