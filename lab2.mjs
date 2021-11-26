import fetch from 'node-fetch';
import fs from 'fs';
import { exec as execCallback } from 'child_process';
import util from 'util';

const VACCINES_LINK = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-20/master/data_tables/vaccine_data/global_data/time_series_covid19_vaccine_global.csv';
const CASES_LINK_PART1 = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/';
const CASES_LINK_PART2 = '.csv';
const CSV_VACCINES_FILE = './vaccines.csv';
const CSV_CASES_FILE = './cases.csv';

const CLI_HADOOP_CREATE_DIR = 'hadoop fs -mkdir /covid19/';
const CLI_HADOOP_PUT_CSV = `hadoop fs -put ${CSV_VACCINES_FILE} /covid19/vaccines`;
const CLI_DELETE_CSV = `rm ${CSV_VACCINES_FILE}`;
const CLI_POST_PROCESSING = `sed -i '' '/FIPS/d' cases.csv`;

const TIMER_LABEL = 'Done in';

const WriteStreamVaccines = fs.createWriteStream(CSV_VACCINES_FILE);
const WriteStreamCases = fs.createWriteStream(CSV_CASES_FILE, { flags: 'a' });
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

const pullDataReports = async () => {
    let currentDate = new Date(startDate);
    while (currentDate < today) {
        const stream = await fetch(getCasesLink(currentDate));
        await new Promise((resolve, reject) => {
            stream.body
                .pipe(fs.createWriteStream(CSV_CASES_FILE, {flags: 'a'}))
                .on('finish', () => resolve())
                .on('error', err => reject(err));
        });
        currentDate = getTomorrow(currentDate);
    }
};

const pullDataVaccines = () => new Promise((resolve, reject) => {
    fetch(VACCINES_LINK)
        .then(res => {
            res.body.pipe(WriteStreamVaccines)
                .on('finish', () => resolve())
                .on('error', err => reject(err));
        })
        .catch(err => reject(err));
});

const main = async () => {
    try {
        console.time(TIMER_LABEL);
        await Promise.all([
            pullDataVaccines(),
            pullDataReports(),
        ]);
        await exec(CLI_POST_PROCESSING);
        await exec(CLI_HADOOP_CREATE_DIR);
        await exec(CLI_HADOOP_PUT_CSV);
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

main();
