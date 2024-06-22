@set ar6_clean='/Volumes/T7 White/Dataset/AR6/data/data_clean/'
@set ar6_task='/Volumes/T7 White/Dataset/AR6/data/data_task/'

INSTALL postgres;
LOAD postgres;
ATTACH 'dbname=critical_minerals user=postgres host=127.0.0.1' AS db1 (TYPE POSTGRES, READ_ONLY);


CREATE OR REPLACE VIEW trp as (
	select * from read_parquet(${ar6_task} || 'trp_ele_future.parquet')
);


select model, scenario from db1.transform.element_demand_region edr
group by model, scenario
except
select model, scenario from trp 
group by model, scenario;


