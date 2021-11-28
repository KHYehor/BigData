export const VACCINES_LINK = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-20/master/data_tables/vaccine_data/global_data/time_series_covid19_vaccine_global.csv';
export const CASES_LINK_PART1 = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/';
export const CASES_LINK_PART2 = '.csv';
// const CSV_VACCINES_TEMP_FILE = './vaccinesTemp.csv';
export const CSV_VACCINES_FILE = './vaccines.csv';
// const CSV_CASES_TEMP_FILE = './casesTemp.csv';
export const CSV_CASES_FILE = './cases.csv';

export const CLI_HADOOP_REMOVE_DIR = 'hadoop fs -rm -r -f /covid19';
export const CLI_HADOOP_CREATE_DIR = 'hadoop fs -mkdir /covid19/';
export const CLI_HADOOP_PUT_VACCINES = `hadoop fs -put ${CSV_VACCINES_FILE} /covid19/vaccines.csv`;
export const CLI_HADOOP_PUT_CASES = `hadoop fs -put ${CSV_CASES_FILE} /covid19/cases.csv`;
export const CLI_DELETE_CSV = `rm ${CSV_VACCINES_FILE} ${CSV_CASES_FILE}`;
export const CLI_CASES_POST_PROCESSING = `sed -i '/FIPS/d' ${CSV_CASES_FILE}`;