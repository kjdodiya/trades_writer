from gpm_mailer import GPMReportMailer
from datetime import datetime
import argparse


def class_mailer(recipients):
    sender = "trade-reports-mailer@gpm"
    last_working_day = datetime.now().strftime("%d %b %Y")
    file_date = datetime.now().strftime("%d-%b-%Y")
    report_file_name = "Trade-Status-Report-{dt}.csv".format(dt=file_date)
    rptm = GPMReportMailer(sender)
    rptm.compose_email(last_working_day, recipients, report_file_name)
    rptm.send_mail()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send Report to recipients")
    parser.add_argument(
        "-rcpts", "--recipients", required=True, help="Comma separated list of email"
    )

    args = parser.parse_args()
    recipients = args.recipients.split(",")
    class_mailer(recipients)
