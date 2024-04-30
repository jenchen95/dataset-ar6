## src
csv2parquet.py: converting raw AR6 data to parquet format

task_ev.py: r10 population and gdp data

## data

### data_clean

r10.parquet

r5.parquet

r6.parquet

world.parquet

### data_raw

### data_import
From project critical_minerals:
    These are two loop check from it. One is for EV and the other is for Wind.
    ev_ms_check.csv
    wind_ms_check.csv

### data_task

- task: EV

​	gdp_per_cap_future.parquet: C1-C4 gdp and population, r10

   trp_ele_future: r10 transport electricity demand

   r10_cap_renew.parquet: r10 renewable capacity

   r10_supply_ele.parquet: r10 electricity supply

   r10_emissions.parquet: r10 emissions containing total, ele, trp, ccs_ele, and ele_bau


   