import fetch from 'node-fetch';
import fs from 'fs';
import { exec as execCallback } from 'child_process';
import util from 'util';

// CASES TABLE INIT SCRIPTS;
import {
    CLI_SET_VARS,
    CLI_DROP_TABLE_CASES,
    CLI_DROP_TEMP_CASES,
    CLI_INSERT_CASES,
    CLI_LOAD_DATA_CASES,
    CLI_CREATE_TABLE_CASES_TEMP,
    CLI_CREATE_TABLE_CASES,
} from './scripts/cases.hive.js';

// HADOOP INIT SCRIPTS;
import {
    CLI_CASES_POST_PROCESSING,
    CLI_HADOOP_REMOVE_DIR,
    CLI_HADOOP_CREATE_DIR,
    CLI_DELETE_CSV,
    CLI_HADOOP_PUT_VACCINES,
    CLI_HADOOP_PUT_CASES,
} from './scripts/hadoop.init';

// INIT METADATA;
import {
    VACCINES_LINK,
    CSV_CASES_FILE,
    CSV_VACCINES_FILE,
    CASES_LINK_PART1,
    CASES_LINK_PART2,
} from './consts/metadata';

// const CLI_CASES_COLUMN_SWAP = `awk -F ',' 'BEGIN{OFS=",";} {print $1, $2, $3, $4, $6, $7, $8, $9, $10, $11, $12, $13, $14, $5}' ${CSV_CASES_TEMP_FILE} > ${CSV_CASES_FILE}`
// const CLI_VACCINES_COLUMN_SWAP = `awk -F ',' 'BEGIN{OFS=",";} {print $1, $3, $4, $5, $6, $7, $8, $2}' ${CSV_VACCINES_TEMP_FILE} > ${CSV_VACCINES_FILE}`

const TIMER_LABEL = 'Done in';
const TIMER_LABEL_CASES = 'Cases done in';
const TIMER_LABEL_VACCINES = 'Vaccines done in';

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
const runPostProcessing = async () => {
    await exec(CLI_CASES_POST_PROCESSING);
    // await Promise.all([
    //     exec(CLI_CASES_COLUMN_SWAP),
    //     exec(CLI_VACCINES_COLUMN_SWAP),
    // ]);
};

const createHadoopFiles = async () => {
    await exec(CLI_HADOOP_CREATE_DIR);
    await Promise.all([
        exec(CLI_HADOOP_PUT_CASES),
        exec(CLI_HADOOP_PUT_VACCINES),
    ]);
};

const populateHiveCases = async () => {
    console.log('Processing CASES:')
    console.log('Dropping old tables');
    await exec(CLI_DROP_TABLE_CASES);
    console.log('Creating temp table');
    await exec(CLI_CREATE_TABLE_CASES_TEMP);
    console.log('Loading data into temp table');
    await exec(CLI_LOAD_DATA_CASES);
    console.log('Creating table');
    await exec(CLI_CREATE_TABLE_CASES);
    console.log('Setting HIVE vars');
    await exec(CLI_SET_VARS);
    console.log('Transferring data to cases table');
    await exec(CLI_INSERT_CASES);
    console.log('Dropping extra tables');
    await exec(CLI_DROP_TEMP_CASES);
}

const populateHiveVaccines = async () => {
    console.log('Processing VACCINES:')
    console.log('Dropping old tables');
    await exec(CLI_DROP_TABLE_VACCINES);
    console.log('Creating temp table');
    await exec(CLI_CREATE_TABLE_VACCINES_TEMP);
    console.log('Loading data into temp table');
    await exec(CLI_LOAD_DATA_VACCINES);
    console.log('Creating table');
    await exec(CLI_CREATE_TABLE_VACCINES);
    console.log('Setting HIVE vars');
    await exec(CLI_SET_VARS);
    console.log('Transferring data to vaccines table');
    await exec(CLI_INSERT_VACCINES);
    console.log('Dropping extra tables');
    await exec(CLI_DROP_TEMP_VACCINES);
}

const main = async () => {
    try {
        console.time(TIMER_LABEL);
        await preRunCleaning();
        await Promise.all([
            pullAndWriteDataVaccines(),
            pullAndWriteDataCases(),
        ]);
        await runPostProcessing();
        await createHadoopFiles();
        await exec(CLI_DELETE_CSV);
        // TODO: script for creating table with fields
        // Country_Region,Date,Doses_admin,People_partially_vaccinated,People_fully_vaccinated,Report_Date_String,UID,Province_State
        console.timeEnd(TIMER_LABEL);
        console.time(TIMER_LABEL_CASES);
        await populateHiveCases();
        console.timeEnd(TIMER_LABEL_CASES);
        process.exit(0);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
};

main();
