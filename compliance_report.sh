#!/usr/bin/env bash

export SMTP_HOST="10.146.1.21"
readonly sourceFile="/home/kamal/venvs/tradescsv/bin/activate"
REPORT_SRC_PATH="/home/kamal/trades_writer"

source ${sourceFile}
cd ${REPORT_SRC_PATH}

rf=`date +'Trade-Status-Report-%d-%b-%Y.csv'`

# Generate Report
python cards2csv.py

# Send to So.  This should be removed once we are happy with email sendingscripts
python gpm_send_reports.py --recipients mf@global-precious-metals.com,so@global-precious-metals.com
