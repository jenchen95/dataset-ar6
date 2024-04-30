@set r10 = '/Volumes/T7 White/Dataset/AR6/data/data_clean/r10.parquet'
@set r10_supply_ele = '/Volumes/T7 White/Dataset/AR6/data/data_task/r10_supply_ele.parquet'
@set r10_co2 = '/Volumes/T7 White/Dataset/AR6/data/data_task/r10_emissions_co2.parquet'

INSTALL postgres;
LOAD postgres;
ATTACH 'postgresql://postgres:120120@localhost:5432/critical_minerals' AS db (TYPE POSTGRES);

--- table create
-- table: co2 ele
create or replace temp table co2_ele as 
select model, scenario, region, unit, year, value from read_parquet(${r10_co2})
where scope = 'ele';

-- table: supply ele
create or replace temp table sup_ele as
select model, scenario, region, unit, year, value from read_parquet(${r10_supply_ele});



--- ms check
-- ms sup_ele
create or replace temp table ms_sup_ele as 
select model, scenario from sup_ele
group by model, scenario;

-- ms element
create or replace temp table ms_element as 
select model, scenario from db.element
where subdim='total' and model <> 'historical'
group by model, scenario;

-- Check successfully. 474 scenarios
select * from ms_sup_ele 
inner join ms_element using(model, scenario);

