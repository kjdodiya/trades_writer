import smtplib
import email
import os
from email import *
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import loguru
from loguru import logger
from datetime import datetime
from gpm_logger import GPMLogger


class GPMMailer:
    def __init__(self, sender):
        self.__smtp_server = os.environ["SMTP_HOST"]  # Replace with SMTP server IP/host
        self.smtp_obj = smtplib.SMTP(self.__smtp_server)
        self.sender = sender
        self.receivers = None
        self.mail = None
        self.logger = GPMLogger("mail").get_logger()

    def send_mail(self):
        """
        Send emails with or w/o attachment
        """
        try:
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
        self.body_text_fmt = "Dear team,\nPlease find Trade Status Report for the week ended {}.\nThank you."
        self.body_html_fmt = "<html><body><p>Dear team,<br><p>Please find Trade Status Report for the week ended {}.</p><p>Thank you.</p></body></html>"
        GPMMailer.__init__(self, sender)

    def compose_email(self, time_stamp, receivers, report_file):
        self.logger.info(
            "Composing email to send report {rf} for week ending {ts}",
            rf=report_file,
            ts=time_stamp,
        )
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
            self.logger.debug(
                "Could not attach text while sending {rf} {exmsg}",
                rf=report_file,
                exmsg=ex,
            )

        try:
            self.logger.info(
                "Trying to attach {rf} for week ending {ts}",
                rf=report_file,
                ts=time_stamp,
            )
            fp = open(report_file)
            attachment = MIMEApplication(fp.read(), _subtype="csv")
            attachment.add_header(
                "Content-Disposition", "attachment", filename=str(report_file)
            )
            msg.attach(attachment)
            self.mail = msg
            self.logger.info(
                "Report file {rf} attached successfully for week ending {ts}",
                rf=report_file,
                ts=time_stamp,
            )
            self.logger.info(
                "Email composition to send report for week ening {ts} composed successfully",
                ts=time_stamp,
            )
        except Exception as ex:
            self.logger.debug(
                "Could not attach report {rf} {exmsg}", rf=report_file, exmsg=ex
            )
