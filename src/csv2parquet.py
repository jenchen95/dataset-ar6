#%% Imports
import polars as pl

#%% Read CSV
def melt_csv2parquet(scale):
    path_dict = {
        'r10': '../data/data_raw/AR6_Scenarios_Database_R10_regions_v1.1.csv',
        'r5': '../data/data_raw/AR6_Scenarios_Database_R5_regions_v1.1.csv',
        'r6': '../data/data_raw/AR6_Scenarios_Database_R6_regions_v1.1.csv',
        'world': '../data/data_raw/AR6_Scenarios_Database_World_v1.1.csv'
    }
    df = pl.scan_csv(path_dict[scale])
    category = pl.scan_csv('../data/data_raw/category.csv')
    df = (
        df.melt(id_vars=['Model', 'Scenario', 'Region', 'Variable', 'Unit'], variable_name='year', value_name='value')
        .rename({'Model': 'model', 'Scenario': 'scenario', 'Region': 'region', 'Variable': 'variable', 'Unit': 'unit'})
        .drop_nulls(subset=['value'])
        .cast({'year': pl.Int32, 'value': pl.Float64})
    )
    # join category
    df = (
        df.join(category, on=['model', 'scenario'], how='left')
        .select(
            pl.col('category','model','scenario','region','variable','unit','year','value')
        )
    )

    df.sink_parquet('../data/data_clean/' + scale + '.parquet')
    print(scale + ' Done!')

#%% Call function
if __name__ == '__main__':
    melt_csv2parquet('r10')
    melt_csv2parquet('r5')
    melt_csv2parquet('r6')
    melt_csv2parquet('world')