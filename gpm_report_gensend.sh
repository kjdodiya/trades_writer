#!/usr/bin/env bash

export SMTP_HOST="10.146.1.21"
readonly sourceFile="/home/kamal/venvs/tradescsv-prod/bin/activate"
REPORT_SRC_PATH="/home/kamal/prod/trades_writer"

source ${sourceFile}
cd ${REPORT_SRC_PATH}


recipients="mf@global-precious-metals.com,so@global-precious-metals.com"
dt=`date +'%d-%b-%Y'`

if [[ $# -lt 1 ]] 
then
	echo "Needs report type"
else
	rtype=$1
	echo "Generating $rtype report"
	if [[ $# -lt 2 ]] 
	then
		# Generate Report
		python gpm_csv_generator.py -rt $rtype
	elif [ $2 == "ops" ]
	then
		recipients="operations@global-precious-metals.com"
		dt=`date --date="-2 days"  +'%d-%b-%Y'`
		rf=`date --date="-2 days"  +'Status-Report-%d-%b-%Y.csv'`
		of="${rtype^}-$rf"
		python gpm_csv_generator.py -rt $rtype -o $of 

	fi

	# Send to So.  This should be removed once we are happy with email sendingscripts
	python gpm_send_reports.py --recipients $recipients -rt $rtype -dt $dt
fi	
