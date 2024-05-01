import polars as pl
import numpy as np
from func import fit_spline

# Reading and Filtering
ele_gen = (
    pl.scan_parquet('../data/data_clean/r10.parquet')
    .filter(pl.col('category').is_in(['C1','C2','C3','C4']))
    .filter(pl.col('variable').is_in(['Secondary Energy|Electricity']))
    .select(pl.exclude('category'))
    .sort('model','scenario','region','year')
    .collect()
)
unit_ele = ele_gen['unit'][0]

ele_gen_interp = []
for pairs, df in ele_gen.group_by(['model', 'scenario', 'region', 'variable']):
    print('Interpolating for ele_gen:', pairs)
    year = np.arange(df['year'].min(), df['year'].max() + 1)
    df_interp = (
        pl.DataFrame(
            {
                'model': pairs[0],
                'scenario': pairs[1],
                'region': pairs[2],
                'unit': unit_ele,
                'year': year,
                'value': fit_spline(df['value'], df['year']),
            }
        )
    )
    ele_gen_interp.append(df_interp)

ele_gen = pl.concat(ele_gen_interp)

# BUG fix: WITCH 5.0's EN_NPi2020_800 and EN_NPi2020_900 wrong value
ele_gen = ele_gen.update(
    ele_gen.filter(
    (pl.col('model') == 'WITCH 5.0') & (pl.col('scenario').is_in(['EN_NPi2020_800f','EN_NPi2020_900f']))
    ).with_columns(scenario=pl.when(pl.col('scenario') == 'EN_NPi2020_800f').then(pl.lit('EN_NPi2020_800')).otherwise(pl.lit('EN_NPi2020_900'))).sort(['model','scenario','region','year']),
    on = ['model','scenario','region','year'],
    how = 'left'
)

# Exporting
ele_gen.write_parquet('../data/data_task/r10_supply_ele.parquet')
print('ele_gen done')
