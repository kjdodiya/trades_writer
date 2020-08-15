import smtplib
import email
from email import *
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


def get_email_with_attachment(sender, receivers, subject, filename):
    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = sender
    rcvrs = ""
    rcvrs.join(receivers)
    msg["To"] = rcvrs
    try:
        fp = open(filename)
        # attachment = MIMEText(fp.read(), _siubtype='csv')
        attachment = MIMEApplication(fp.read(), _subtype="csv")
        attachment.add_header(
            "Content-Disposition", "attachment", filename=str(filename)
        )
        msg.attach(attachment)
    except Exception as ex:
        print("Could not attach report")
    return msg


def send_email(sender, receivers, imsg):
    try:
        smtpObj = smtplib.SMTP("10.146.1.21")
        send_success = smtpObj.sendmail(sender, receivers, imsg.as_string())
        print("Successfully sent email")
        print(send_success)
    except SMTPException as se:
        print("Error: unable to send email")
        print(se)
    except Exception as ex:
        print(ex)


def main():
    subject = "Testing-2"
    sender = "kamal@tasks-a921.tld"
    receivers = ["gpmdev@global-precious-metals.com"]

    message = """From: Kamal <kamal@tasks-a921.tld>
    To: GPMDEV <gpmdev@global-precious-metals.com>
    Subject: Testing-2
    
    This is a test e-mail message. Pleaes ignore.
    """

    try:
        smtpObj = smtplib.SMTP("10.146.1.21")
        send_success = smtpObj.sendmail(sender, receivers, message)
        print("Successfully sent email")
        print(send_success)
    except SMTPException as se:
        print("Error: unable to send email")
        print(se)
    except Exception as ex:
        print(ex)


if __name__ == "__main__":
    #    main()
    subject = "Test with Attachment-1"
    sender = "kamal@tasks-a921.tld"
    receivers = ["gpmdev@global-precious-metals.com"]
    attach_file = "test_attach.csv"
    msg = get_email_with_attachment(sender, receivers, subject, attach_file)
    send_email(sender, receivers, msg)
