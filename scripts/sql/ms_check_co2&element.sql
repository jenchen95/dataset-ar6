@set r10_co2 = '/Volumes/T7 White/Dataset/AR6/data/data_task/r10_emissions_co2.parquet'
@set element_dem = '/Users/chen/Code/critical_minerals/data/data_output/element_demand.parquet'
@set r10 = '/Volumes/T7 White/Dataset/AR6/data/data_clean/r10.parquet'


-- check co2 total, ele, trp
create or replace view ms_co2 as
select distinct model, scenario from read_parquet(${r10_co2})
--- where scope = 'total'; -- or ele, trp


create or replace view ms_element as
SELECT model, scenario FROM read_parquet(${element_dem})
		WHERE dim = 'demand' AND subdim <> 'total' AND model <> 'historical'
		GROUP BY model, scenario;

select count(*) from ms_co2 mc
inner join ms_element me using(model, scenario);


-- check ccs ele model scenario
select count(*) from(
select distinct model, scenario from read_parquet(${r10})
inner join ms_element me using (model, scenario)
where variable = 'Carbon Sequestration|CCS|Fossil|Energy|Supply|Electricity'
);


-- see if value is positive for ccs ele
select * from read_parquet(${r10})
where variable = 'Carbon Sequestration|CCS|Fossil|Energy|Supply|Electricity'
limit 100;