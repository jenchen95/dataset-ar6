import polars as pl 

# Reading and Filtering
cap_var = ['Capacity|Electricity|Solar|PV', 'Capacity|Electricity|Solar|CSP', 'Capacity|Electricity|Wind|Onshore', 'Capacity|Electricity|Wind|Offshore', 'Capacity|Electricity|Wind']

ms_wind = pl.read_csv('../data/data_import/wind_ms_check.csv')

cap = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(cap_var))
    .select(pl.exclude('category'))
    .with_columns(
        pl.when(
            (pl.col('model').is_in(ms_wind['model'])) & 
            (pl.col('scenario').is_in(ms_wind['scenario'])) &
            (pl.col('variable') == 'Capacity|Electricity|Wind')
            )
        .then(
            pl.lit('Capacity|Electricity|Wind|Onshore')
            )
        .otherwise(pl.col('variable'))
        .alias('variable')
    )
)

# Exporting
cap.sink_parquet('../data/data_task/r10_cap_renew.parquet')