from gpm_mailer import GPMReportMailer, GPMComplianceMailer
from datetime import datetime, timedelta
import argparse


def trade_report_mailer(recipients, fdt):
    sender = "reports-mailer@gpm"
    file_date = fdt
    last_working_day = fdt.replace("-", " ")  # ("%d %b %Y")
    report_file_name = "Trade-Status-Report-{dt}.csv".format(dt=file_date)
    rptm = GPMReportMailer(sender)
    rptm.compose_email(last_working_day, recipients, report_file_name)
    # rptm.send_mail()


def compliance_report_mailer(recipients, fdt):
    sender = "reports-mailer@gpm"
    file_date = fdt
    last_working_day = fdt.replace("-", " ")  # ("%d %b %Y")
    report_file_name = "Compliance-Status-Report-{dt}.csv".format(dt=file_date)
    rptm = GPMComplianceMailer(sender)
    rptm.compose_email(last_working_day, recipients, report_file_name)
    rptm.send_mail()


if __name__ == "__main__":
    last_working_day = datetime.now().strftime("%d %b %Y")
    file_date = datetime.now().strftime("%d-%b-%Y")

    parser = argparse.ArgumentParser(description="Send Report to recipients")
    rcpt_group = parser.add_mutually_exclusive_group(required=True)
    rcpt_group.add_argument(
        "-rcpts", "--recipients", help="Comma separated list of email"
    )
    rcpt_group.add_argument(
        "-rcpts_from_file",
        "--recipients_from_file",
        help="File containing email ids (one per line)",
    )
    parser.add_argument(
        "-dt",
        "--date",
        help="Date of the report to send (DD-MMM-YYYY) e.g 24-Aug-2020",
        default=file_date,
    )
    parser.add_argument(
        "-rt", "--report_type", help="Type of report (trade/compliance)", required=True
    )

    args = parser.parse_args()

    file_date = args.date
    report_type = args.report_type
    recipients = args.recipients.split(",")
    if report_type == "trade":
        trade_report_mailer(recipients, file_date)
    elif report_type == "compliance":
        compliance_report_mailer(recipients, file_date)
