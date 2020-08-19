import smtplib
import email
from email import *
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import loguru
from datetime import datetime


class GPMMailer:
    def __init__(self, sender):
        self.__smtp_server = "X.X.X.X"  # Replace with SMTP server IP/host
        self.smtp_obj = smtplib.SMTP(self.__smtp_server)
        self.sender = sender
        self.receivers = None
        self.mail = None

    def send_mail(self):
        """
        Send emails with or w/o attachment
        """
        try:
            print(self.mail.as_string())
            print(self.receivers)
            print(self.sender)
            send_status = self.smtp_obj.sendmail(
                self.sender, self.receivers, self.mail.as_string()
            )
            print("Successfully sent email")
            print(send_status)
        except SMTPException as se:
            print("Error: unable to send email")
            print(se)
        except Exception as ex:
            print(ex)


class GPMReportMailer(GPMMailer):
    def __init__(self, sender):
        self.subject_fmt = "Trade Status Report - {}"
        self.message_body_fmt = "(--- TESTING IGNORE ---)\nDear team, please find Trade Status Report for the week ended {}.\nThank you."
        self.body_html_fmt = """<html><body>
                <p>Dear team,<br>
                <p>Please find Trade Status Report for the week ended {}.</p> 
                <p>Thank you.</p> 
              </body>
            </html>"""
        GPMMailer.__init__(self, sender)

    def compose_email(self, time_stamp, receivers, report_file):
        self.receivers = receivers
        msg = MIMEMultipart()
        msg["Subject"] = self.subject_fmt.format(time_stamp)
        msg["From"] = self.sender
        rcvrs = ""
        rcvrs.join(receivers)
        msg["To"] = rcvrs
        try:
            message_body = self.body_html_fmt.format(time_stamp)
            mime_html = MIMEText(message_body, "html")
            msg.attach(mime_html)
        except Exception as ex:
            print("Could not attach text")

        try:
            fp = open(report_file)
            attachment = MIMEApplication(fp.read(), _subtype="csv")
            attachment.add_header(
                "Content-Disposition", "attachment", filename=str(report_file)
            )
            msg.attach(attachment)
            self.mail = msg

        except Exception as ex:
            print("Could not attach report")
