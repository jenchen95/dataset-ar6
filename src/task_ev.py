# Import
import polars as pl


# Reading
pop = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable') == 'Population')
)

gdp = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(['GDP|PPP', 'GDP|MER']))
)


# Export
pop.sink_parquet('../data/data_task/population.parquet')
print('Population data exported')
gdp.sink_parquet('../data/data_task/gdp.parquet')
print('GDP data exported')