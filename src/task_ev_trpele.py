# Import
import polars as pl
import numpy as np
import scipy

r10_list = pl.read_csv('../data/data_man/r10_list.csv')

trp_ele = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(
        ['Final Energy|Transportation|Electricity', 
         'Final Energy|Transportation|Passenger|Electricity', 
         'Final Energy|Transportation|Freight|Electricity',
         'Final Energy|Transportation|Rail']
    ))
    .collect()
    .pivot(index=['category','model','scenario','region','year'], columns='variable', values='value')
    .drop_nulls(subset=['Final Energy|Transportation|Electricity'])
    .join(
        pl.read_csv('../data/data_man/trpele_exctrain_ratio.csv').select(pl.col('region','ratio')),
        on = ['region']
    )
    .with_columns(
        trp_ele_ev=pl.col('Final Energy|Transportation|Electricity') * pl.col('ratio'),
        pas_ele=pl.col('Final Energy|Transportation|Passenger|Electricity'),
        fre_ele=pl.col('Final Energy|Transportation|Freight|Electricity')
    )
    .with_columns(unit=pl.lit('EJ'))
    .select(pl.col('category','model','scenario','region','year','unit','trp_ele_ev','pas_ele','fre_ele'))
)

trp_ele.write_parquet('../data/data_task/trp_ele_future.parquet')
print('Final Energy|Transportation|Electricity data exported')

