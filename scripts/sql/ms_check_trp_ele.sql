@set ar6_clean='/Volumes/T7 White/Dataset/AR6/data/data_clean/'
@set ar6_task='/Volumes/T7 White/Dataset/AR6/data/data_task/'

INSTALL postgres;
LOAD postgres;
ATTACH 'dbname=critical_minerals user=postgres host=127.0.0.1' AS db1 (TYPE POSTGRES, READ_ONLY);


CREATE OR REPLACE VIEW trp_ele as (
	select * from read_parquet(${ar6_task} || 'trp_ele_future.parquet')
);


select model, scenario from db1.transform.element_demand_region edr
group by model, scenario
except
select model, scenario from trp_ele
group by model, scenario;

CREATE or replace temp table trp_check_rdft as (
	select model, scenario, variable from read_parquet(${ar6_clean} || 'r10.parquet')
	where 
		variable = 'Final Energy|Transportation|Road|Freight'
		and category in ('C1', 'C2', 'C3', 'C4')
	group by model, scenario, variable
);

CREATE or replace temp table trp_check_ftele as (
	select model, scenario, variable from read_parquet(${ar6_clean} || 'r10.parquet')
	where 
		variable = 'Final Energy|Transportation|Freight|Electricity'
		and category in ('C1', 'C2', 'C3', 'C4')
	group by model, scenario, variable
);



CREATE or replace temp table trp_check_rdps as (
	select model, scenario, variable from read_parquet(${ar6_clean} || 'r10.parquet')
	where 
		variable = 'Final Energy|Transportation|Road|Passenger'
		and category in ('C1', 'C2', 'C3', 'C4')
	group by model, scenario, variable
);

CREATE or replace temp table trp_check_psele as (
	select model, scenario, variable from read_parquet(${ar6_clean} || 'r10.parquet')
	where 
		variable = 'Final Energy|Transportation|Passenger|Electricity'
		and category in ('C1', 'C2', 'C3', 'C4')
	group by model, scenario, variable
);


select model, scenario from trp_check_rdft;  -- 32
select model, scenario from trp_check_rdps;  -- 32

select model, scenario from trp_check_ftele; -- 157
select model, scenario from trp_check_psele; -- 280

select model, scenario from trp_ele  -- 512, contain above four groups
group by model, scenario 
except
select model, scenario from trp_check_rdps;





