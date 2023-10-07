import os
import smtplib
from email.message import EmailMessage
from dotenv import load_dotenv

load_dotenv()

def send_email(sender_email, sender_password, recipient_email, subject, message):
    try:
        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = recipient_email
        msg.set_content(message)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(sender_email, sender_password)
            smtp.send_message(msg)
        return True
    except Exception as e:
        print("Problem during send email")
        print(str(e))
    return False

# Manually input email addresses and password
sender_email = input("Enter your email address: ")
sender_password = input("Enter your email password: ")
recipient_email = input("Enter recipient's email address: ")

if send_email(sender_email, sender_password, recipient_email, "Test Email", "This is a test email from Python."):
    print("Email sent successfully!")
else:
    print("Email sending failed.")
