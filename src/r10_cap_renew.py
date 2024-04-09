import polars as pl 

# Reading
cap_var = ['Capacity|Electricity|Solar|PV', 'Capacity|Electricity|Solar|CSP', 'Capacity|Electricity|Wind|Onshore', 'Capacity|Electricity|Wind|Offshore', 'Capacity|Electricity|Wind']

# Filtering
cap = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(cap_var))
    .select(pl.exclude('category'))
)

# Exporting
cap.sink_parquet('../data/data_task/r10_cap_renew.parquet')