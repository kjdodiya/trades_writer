#!/usr/bin/env bash


readonly sourceFile="/home/kamal/venvs/tradescsv/bin/activate"
REPORT_SRC_PATH="/home/kamal/trades_writer"

export SMTP_HOST="10.146.1.21"

source ${sourceFile}
cd ${REPORT_SRC_PATH}

echo ${SMTP_HOST}

dt=`date --date="-2 days"  +'%d-%b-%Y'`
rf=`date --date="-2 days"  +'Trade-Status-Report-%d-%b-%Y.csv'`


# Generate Report
python cards2csv.py -o $rf
# Send to So.  This should be removed once we are happy with email sendingscripts
python gpm_send_reports.py --recipients operations@global-precious-metals.com -dt $dt

