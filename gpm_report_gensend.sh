#!/usr/bin/env bash

export SMTP_HOST="10.146.1.21"
readonly sourceFile="/home/kamal/venvs/tradescsv/bin/activate"
REPORT_SRC_PATH="/home/kamal/trades_writer"

source ${sourceFile}
cd ${REPORT_SRC_PATH}

if [[ $# -lt 1 ]] 
then
	echo "Needs report type"
else
	rtype=$1
	echo "Generating $rtype report"

	# Generate Report
	python gpm_csv_generator.py -rt $rtype

	# Send to So.  This should be removed once we are happy with email sendingscripts
	python gpm_send_reports.py --recipients mf@global-precious-metals.com,so@global-precious-metals.com -rt $rtype
fi	
