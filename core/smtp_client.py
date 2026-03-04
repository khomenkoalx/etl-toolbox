import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from core.config import settings


def send_notification(text, subject):

    msg = MIMEMultipart()
    msg['From'] = settings.email_sender
    msg['To'] = settings.email_receiver
    msg['Subject'] = subject

    msg.attach(MIMEText(text, 'plain', 'utf-8'))

    try:
        server = smtplib.SMTP(
            settings.smtp_host,
            settings.smtp_port
        )
        server.starttls()
        server.login(
            settings.email_sender,
            settings.email_password
        )
        server.send_message(
            msg,
            to_addrs=settings.email_receiver
        )
        server.quit()
    except Exception as e:
        print(f'Возникла ошибка: {e}')
