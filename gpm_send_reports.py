from gpm_mailer import GPMReportMailer
from datetime import datetime, timedelta
import argparse


def class_mailer(recipients, fdt):
    sender = "trade-reports-mailer@gpm"
    file_date = fdt
    last_working_day = fdt.replace("-", " ")  # ("%d %b %Y")
    report_file_name = "Trade-Status-Report-{dt}.csv".format(dt=file_date)
    rptm = GPMReportMailer(sender)
    rptm.compose_email(last_working_day, recipients, report_file_name)
    rptm.send_mail()


if __name__ == "__main__":
    last_working_day = datetime.now().strftime("%d %b %Y")
    file_date = datetime.now().strftime("%d-%b-%Y")

    parser = argparse.ArgumentParser(description="Send Report to recipients")
    parser.add_argument(
        "-rcpts", "--recipients", required=True, help="Comma separated list of email"
    )
    parser.add_argument(
        "-dt", "--date", help="Date of the report to send", default=file_date
    )

    args = parser.parse_args()

    file_date = args.date
    recipients = args.recipients.split(",")

    class_mailer(recipients, file_date)
