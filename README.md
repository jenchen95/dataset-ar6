# Dataset of IPCC AR6 scenario database
## src
csv2parquet.py: converting raw AR6 data to parquet format

task_ev_gdp.py: r10 population and gdp data

task_ev_trpele.py: r10 transport electricity demand for ev

## data

### data_clean

r10.parquet

r5.parquet

r6.parquet

world.parquet

### data_raw

### data_man
r10_list.csv: list of r10 regions
ssp_family.csv: list of ssp families
trpele_exctrain_ratio.csv: ratio of train electricity demand to total electricity demand

### data_import
From project critical_minerals:
    These are two loop check from it. One is for EV and the other is for Wind.
    ev_ms_check.csv
    wind_ms_check.csv

### data_task

- task: EV

​	gdp_per_cap_future.parquet: C1-C4 gdp and population, r10

   trp_ele_future.parquet: r10 transport electricity demand for ev

   r10_cap_renew.parquet: r10 renewable capacity

   r10_supply_ele.parquet: r10 electricity supply

   r10_emissions_co2.parquet: r10 emissions containing total, ele, trp, ccs_ele, and ele_bau

   r10_abate_ratio_by_ele.parquet: r10 abatement contribution by power technologies' power grow volume compared to 2020.