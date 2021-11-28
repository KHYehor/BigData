import fetch from 'node-fetch';
import fs from 'fs';
import { exec as execCallback } from 'child_process';
import util from 'util';

const VACCINES_LINK = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-20/master/data_tables/vaccine_data/global_data/time_series_covid19_vaccine_global.csv';
const CASES_LINK_PART1 = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/';
const CASES_LINK_PART2 = '.csv';
const CSV_VACCINES_FILE = './vaccines.csv';
const CSV_CASES_FILE = './cases.csv';

const CLI_HADOOP_REMOVE_DIR = 'hadoop fs -rm -r -f /covid19';
const CLI_HADOOP_CREATE_DIR = 'hadoop fs -mkdir /covid19/';
const CLI_HADOOP_PUT_VACCINES = `hadoop fs -put ${CSV_VACCINES_FILE} /covid19/vaccines`;
const CLI_HADOOP_PUT_CASES = `hadoop fs -put ${CSV_CASES_FILE} /covid19/cases`;
const CLI_DELETE_CSV = `rm ${CSV_VACCINES_FILE} ${CSV_CASES_FILE}`;
const CLI_POST_PROCESSING = `sed -i '/FIPS/d' cases.csv`;

const TIMER_LABEL = 'Done in';

const WriteStreamVaccines = fs.createWriteStream(CSV_VACCINES_FILE);
const getWriteStreamCases = () => fs.createWriteStream(CSV_CASES_FILE, {flags: 'a'});
const exec = util.promisify(execCallback);

const today = new Date();
today.setUTCHours(0,0,0, 0);

const startDate = new Date(today);
startDate.setUTCFullYear(2021, 10, 1);
startDate.setUTCHours(0, 0, 0, 0);

const getTomorrow = (date) => {
    const temp = new Date(date);
    temp.setDate(temp.getDate()+1);
    return temp;
};

const getYesterday = (date) => {
    const temp = new Date(date);
    temp.setDate(temp.getDate()-1);
    return temp;
};

const getCasesLink = (date) => `${CASES_LINK_PART1}${(date.getMonth()+1).toString().padStart(2, '0')}-${date.getDate().toString().padStart(2, '0')}-${date.getFullYear()}${CASES_LINK_PART2}`;

const pullAndWriteDataCases = async () => {
    let currentDate = new Date(startDate);
    while (currentDate < today) {
        const stream = await fetch(getCasesLink(currentDate));
        await new Promise((resolve, reject) => {
            stream.body
                .pipe(getWriteStreamCases())
                .on('finish', () => resolve())
                .on('error', err => reject(err));
        });
        currentDate = getTomorrow(currentDate);
    }
};

const pullAndWriteDataVaccines = () => new Promise((resolve, reject) => {
    fetch(VACCINES_LINK)
        .then(res => {
            res.body.pipe(WriteStreamVaccines)
                .on('finish', () => resolve())
                .on('error', err => reject(err));
        })
        .catch(err => reject(err));
});

const preRunCleaning = async () => exec(CLI_HADOOP_REMOVE_DIR);

const main = async () => {
    try {
        console.time(TIMER_LABEL);
        await preRunCleaning();
        await Promise.all([
            pullAndWriteDataVaccines(),
            pullAndWriteDataCases(),
        ]);
        await exec(CLI_POST_PROCESSING);
        await exec(CLI_HADOOP_CREATE_DIR);
        await Promise.all([
            exec(CLI_HADOOP_PUT_CASES),
            exec(CLI_HADOOP_PUT_VACCINES),
        ]);
        await exec(CLI_POST_PROCESSING);
        await exec(CLI_DELETE_CSV);
        // TODO: script for creating table with fields
        // Country_Region,Date,Doses_admin,People_partially_vaccinated,People_fully_vaccinated,Report_Date_String,UID,Province_State
        console.timeEnd(TIMER_LABEL);
        process.exit(0);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
};


const CLI_DROP_TABLE_CASES = `drop table cases;`;
const CLI_CREATE_TABLE_CASES = `
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
    Case_Fatality_Ratio DOUBLE,
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
PARTITIONED BY (Last_Update TIMESTAMP);
LOCATION /covid19/db;
`
main();
