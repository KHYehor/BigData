export const CLI_DROP_RECREATE_TABLES_CASES = `
hive -e " 
drop table cases_temp;
drop table cases;
create external table cases_temp
(
    FIPS_Code string,
    Admin2 string,
    Province_State string,
    Country_Region string,
    Last_Update TIMESTAMP,
    Lat DOUBLE,
    Long DOUBLE,
    Confirmed INTEGER,
    Deaths INTEGER,
    Recovered INTEGER,
    Active INTEGER,
    Combined_Key string,
    Incident_Rate DOUBLE,
    Case_Fatality_Ratio DOUBLE
)
CLUSTERED BY (Country_Region) INTO 10 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/covid19/cases_table_temp/';
create external table cases
(
    FIPS_Code string,
    Admin2 string,
    Province_State string,
    Country_Region string,
    Last_Update TIMESTAMP,
    Lat DOUBLE,
    Long DOUBLE,
    Confirmed INTEGER,
    Deaths INTEGER,
    Recovered INTEGER,
    Active INTEGER,
    Combined_Key string,
    Incident_Rate DOUBLE,
    Case_Fatality_Ratio DOUBLE
)
PARTITIONED BY (Month INTEGER)
CLUSTERED BY (Country_Region) INTO 10 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/covid19/cases_table/';
"
`;
export const CLI_LOAD_DATA_CASES = `
hive -e "
LOAD DATA INPATH '/covid19/cases.csv' INTO TABLE cases_temp;
"
`;
export const CLI_INSERT_CASES = `
hive -e '
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into cases partition(Month) select *, MONTH(Last_Update) as Month from cases_temp;
'
`;
export const CLI_DROP_TEMP_CASES = `
hive -e 'drop table cases_temp;'
hadoop fs -rm -r -f /covid19/cases_table_temp;
`;