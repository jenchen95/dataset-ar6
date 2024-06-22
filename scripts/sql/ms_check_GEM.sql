@set ar6_clean='/Volumes/T7 White/Dataset/AR6/data/data_clean/'
@set ar6_task='/Volumes/T7 White/Dataset/AR6/data/data_task/'



SELECT distinct scenario from read_parquet(${ar6_task} || 'r10_cap_renew.parquet')
WHERE model = 'GEM-E3_V2021';

SELECT distinct scenario from read_parquet(${ar6_task} || 'r10_cap_renew.parquet')
WHERE model = 'GEM-E3_V2021' and region = 'R10AFRICA';