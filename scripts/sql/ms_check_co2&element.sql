@set r10_co2 = '/Volumes/T7 White/Dataset/AR6/data/data_task/r10_emissions_co2.parquet'
@set r10_co2_ele = '/Volumes/T7 White/Dataset/AR6/data/data_task/r10_emissions_co2_ele.parquet'
@set r10_co2_trp = '/Volumes/T7 White/Dataset/AR6/data/data_task/r10_emissions_co2_trp.parquet'
@set element_dem = '/Users/chen/Code/critical_minerals/data/data_output/element_demand.parquet'


create or replace view ms_co2 as
select distinct model, scenario from read_parquet(${r10_co2});

create or replace view ms_co2_ele as
select distinct model, scenario 
from read_parquet(${r10_co2_ele})
order by model;


create or replace view ms_co2_trp as
select distinct model, scenario 
from read_parquet(${r10_co2_trp})
order by model;


create or replace view ms_element as
SELECT model, scenario FROM read_parquet(${element_dem})
		WHERE dim = 'demand' AND subdim <> 'total' AND model <> 'historical'
		GROUP BY model, scenario;

select count(*) from ms_co2 mc
inner join ms_element me using(model, scenario);

select count(*) from ms_co2_ele mel
inner join ms_element me using(model, scenario);

select count(*) from ms_co2_trp mtr
inner join ms_element me using(model, scenario);