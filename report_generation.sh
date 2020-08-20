#!/usr/bin/env bash

readonly sourceFile="/home/kamal/venvs/tradescsv/bin/activate"
REPORT_SRC_PATH="/home/kamal/trades_writer"

source ${sourceFile}
cd ${REPORT_SRC_PATH}

# Generate Report
python cards2csv.py

# Send to So.  This should be removed once we are happy with email sendingscripts
python gpm_send_reports.py --recipients so@global-precious-metals.com

rf=`date +'Trade-Status-Report-%d-%b-%Y.csv'`

cp $rf  $rf".orig"

# Generate file with dummy content for gpmdev DL
# This should not contain actual data
dummy_line="h1,h2,h3,h4,h5\nd11,d12,d13,d14,d15\nd21,d22,d23,d24,d25"



echo -e $dummy_line > $rf

# Send this dummmy report to the gpmdev DL
python gpm_send_reports.py --recipients gpmdev@global-precious-metals.com
