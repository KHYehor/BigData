import fetch from 'node-fetch';
import fs from 'fs';
import { exec } from 'child_process';

const LINK_TO_PULL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-20/master/data_tables/vaccine_data/global_data/time_series_covid19_vaccine_global.csv';
const CSV_DATA_FILE = './vaccines.csv';
const HADOOP_CREATE_DIR = 'hadoop fs -mkdir /covid19/vaccines';
const HADOOP_PUT_DOC = 'hadoop fs -put ./covid19/vaccines.csv /covid19/vaccines';
const TIMER_LABEL = 'Done in';

const WriteStream = fs.createWriteStream(CSV_DATA_FILE);

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
        exec(HADOOP_CREATE_DIR);
        exec(HADOOP_PUT_DOC);
        console.timeEnd(TIMER_LABEL);
        process.exit(0);
    } catch (err) {
        console.error(err);
        process.exit(1);
    }
};

main();
