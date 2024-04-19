"""
version: 1.0, date: 2023-11-2
Class This class send notification by email, in the future try to send notification throw cell

developer by darwin11rv@gmail.com
Copyright. INAMHI @ 2023 <www.inamhi.gob.ec>. all rights reserved.
"""

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import utils.manage_conf as conf


class Sender(object):
    def __init__(self):
        pass

    def mail_notifier(self, smtp_server, sender, password, recipients, subject, mensaje):
        # make MIME
        print("enviando notificacion")
        mensaje_mime = MIMEMultipart("alternative")
        mensaje_mime["From"] = sender
        mensaje_mime["To"] = recipients
        mensaje_mime["Subject"] = subject
        # Crea el cuerpo del mensaje en HTML
        mensaje_html = MIMEText(mensaje, "html")
        mensaje_mime.attach(mensaje_html)

        # server conection
        with smtplib.SMTP(smtp_server, 587) as smtp:
            smtp.starttls()
            smtp.login(sender, password)
            smtp.sendmail(sender, recipients, mensaje_mime.as_string())



if __name__ == '__main__':
    #

    s = Sender()
    enoti = conf.get_cred(fileconf="../config.ini", section='email_noti_conf')
    # print(enoti['smtp_server'], " - ", enoti['port'], " - ", enoti['sender'], " - ", enoti['password'], " - ",
    #       enoti['recipies'])
    emessage = conf.get_cred(fileconf="../config.ini", section='email_noti_log_mess')
    # print(emessage['subject'], " - ", emessage['message'])

    s.mail_notifier(enoti['smtp_server'], enoti['sender'], enoti['password'], enoti['recipies'],emessage['subject'],
                    emessage['message'])

    emessage = conf.get_cred(fileconf="../config.ini", section='email_noti_chp_mess')
    # print(emessage['subject'], " - ", emessage['message'])

    s.mail_notifier(enoti['smtp_server'], enoti['sender'], enoti['password'], enoti['recipies'], emessage['subject'],
                    emessage['message'])