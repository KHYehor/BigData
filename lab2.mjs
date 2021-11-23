import fetch from 'node-fetch';
import fs from 'fs';
import { exec as execCallback } from 'child_process';
import util from 'util';

const LINK_TO_PULL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-20/master/data_tables/vaccine_data/global_data/time_series_covid19_vaccine_global.csv';
const CSV_DATA_FILE = './vaccines.csv';

const CLI_HADOOP_CREATE_DIR = 'hadoop fs -mkdir /covid19/';
const CLI_HADOOP_PUT_CSV = `hadoop fs -put ${CSV_DATA_FILE} /covid19/vaccines`;
const CLI_HADOOP_DELETE_CSV = `rm ${CSV_DATA_FILE}`;

const TIMER_LABEL = 'Done in';

const WriteStream = fs.createWriteStream(CSV_DATA_FILE);
const exec = util.promisify(execCallback);

const pullData = () => new Promise((resolve, reject) => {
    fetch(LINK_TO_PULL)
        .then(res => {
            res.body.pipe(WriteStream)
                .on('finish', () => resolve())
                .on('error', err => reject(err));
        })
        .catch(err => reject(err));
});

const main = async () => {
    try {
        console.time(TIMER_LABEL);
        await pullData();
        await exec(CLI_HADOOP_CREATE_DIR);
        await exec(CLI_HADOOP_PUT_CSV);
        await exec(CLI_HADOOP_DELETE_CSV);
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
