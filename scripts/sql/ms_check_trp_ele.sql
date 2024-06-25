@set ar6_clean='/Volumes/T7 White/Dataset/AR6/data/data_clean/'
@set ar6_task='/Volumes/T7 White/Dataset/AR6/data/data_task/'

INSTALL postgres;
LOAD postgres;
ATTACH 'dbname=critical_minerals user=postgres host=127.0.0.1' AS db1 (TYPE POSTGRES, READ_ONLY);


CREATE OR REPLACE temp table trp_ele as (
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

create or replace temp table trp_check_vkm as (
	select model, scenario, variable from read_parquet(${ar6_clean} || 'r10.parquet')
	where 
		variable = 'Energy Service|Transportation|Road'
		and category in ('C1', 'C2', 'C3', 'C4')
	group by model, scenario, variable
)


select model, scenario from trp_check_vkm  -- 6, all REMIND-Transport 2.1
group by model, scenario;

create or replace temp table trp_check_road_ftele as (
	select model, scenario, variable from read_parquet(${ar6_clean} || 'r10.parquet')
	where 
		variable = 'Final Energy|Transportation|Road|Freight|Electric'
		and category in ('C1', 'C2', 'C3', 'C4')
	group by model, scenario, variable
)

create or replace temp table trp_road_ftele as (
	select 
		tr.category as category,
		tr.model as model,
		tr.scenario as scenario,
		tr.region as region,
		tr.variable as variable,
		tr.unit as unit,
		tr.year as year,
		tr.value as value
	from read_parquet(${ar6_clean} || 'r10.parquet') tr
	right join trp_check_road_ftele tc using (model, scenario)
	where  
		tr.variable = 'Final Energy|Transportation|Road|Freight|Electric'
		and tr.category in ('C1', 'C2', 'C3', 'C4')
		and tc.model not null and tc.scenario not null
)

select * from trp_road_ftele
left join trp_ele using (model, scenario, region, year)
;
create or replace temp table trp_check_rail as (
	select model, scenario, variable from read_parquet(${ar6_clean} || 'r10.parquet')
	where 
		variable = 'Final Energy|Transportation|Rail'
		and category in ('C1', 'C2', 'C3', 'C4')
	group by model, scenario, variable
)

create or replace temp table trp_rail as (
	select 
		tr.category as category,
		tr.model as model,
		tr.scenario as scenario,
		tr.region as region,
		tr.variable as variable,
		tr.unit as unit,
		tr.year as year,
		tr.value as value
	from read_parquet(${ar6_clean} || 'r10.parquet') tr
	right join trp_check_rail tc using (model, scenario)
	where  
		tr.variable = 'Final Energy|Transportation|Rail'
		and tr.category in ('C1', 'C2', 'C3', 'C4')
		and tc.model not null and tc.scenario not null
)

select * from trp_rail
left join trp_ele using (model, scenario, region, year)
where region = 'R10CHINA+'
;


create or replace temp table trp_stock_road as (
	select model, scenario, variable from read_parquet(${ar6_clean} || 'r10.parquet')
	where 
		variable = 'Transport|Stock|Road'  
	group by model, scenario, variable
);
select * from trp_stock_road; -- just several model

