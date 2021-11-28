export const CLI_DROP_TABLE_VACCINES = `
hive -e 'drop table vaccines_temp;'
hive -e 'drop table vaccines;'
`;
export const CLI_CREATE_TABLE_VACCINES_TEMP = `
create external table vaccines_temp
(
    Country_Region STRING,
    Date DATE,
    Doses_admin INTEGER,
    People_partially_vaccinated INTEGER,
    People_fully_vaccinated INTEGER,
    Report_Date_String DATE,
    UID STRING,
    Province_State STRING
)
CLUSTERED BY (Country_Region) INTO 10 BUCKETS
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/covid19/vaccines_table_temp';
`;
export const CLI_LOAD_DATA_VACCINES = `
hive -e "
LOAD DATA INPATH '/covid19/vaccines.csv' INTO TABLE vaccines_temp;
"
`;
export const CLI_CREATE_TABLE_VACCINES = `
create external table vaccines
(
    Country_Region STRING,
    Date DATE,
    Doses_admin INTEGER,
    People_partially_vaccinated INTEGER,
    People_fully_vaccinated INTEGER,
    Report_Date_String DATE,
    UID STRING,
    Province_State STRING
)
PARTITIONED BY (Month INTEGER)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION '/covid19/vaccines_table';
`;
export const CLI_INSERT_VACCINES = `
hive -e '
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert into vaccines partition(Month) select *, MONTH(Date) as Month from vaccines_temp;
'
`;
export const CLI_DROP_TEMP_VACCINES = `
hive -e 'drop table vaccines_temp;'
hadoop fs -rm -r -f /covid19/vaccines_table_temp;
`;