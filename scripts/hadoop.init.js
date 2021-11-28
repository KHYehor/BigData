export const CLI_HADOOP_REMOVE_DIR = 'hadoop fs -rm -r -f /covid19';
export const CLI_HADOOP_CREATE_DIR = 'hadoop fs -mkdir /covid19/';
export const CLI_HADOOP_PUT_VACCINES = `hadoop fs -put ${CSV_VACCINES_FILE} /covid19/vaccines.csv`;
export const CLI_HADOOP_PUT_CASES = `hadoop fs -put ${CSV_CASES_FILE} /covid19/cases.csv`;
export const CLI_DELETE_CSV = `rm ${CSV_VACCINES_FILE} ${CSV_CASES_FILE}`;
export const CLI_CASES_POST_PROCESSING = `sed -i '/FIPS/d' ${CSV_CASES_FILE}`;