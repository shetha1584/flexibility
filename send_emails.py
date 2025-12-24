import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

SENDER_EMAIL = "testelements13@gmail.com"
APP_PASSWORD = "kmakbshmgwahgndi"

RECIPIENT_EMAIL = "harshetha1674@gmail.com"

# IMPORTANT: use ngrok URL when testing on phone/email
BASE_URL = "http://127.0.0.1:5000/track?client_id=1&shift="

def send_text_email(subject, body):
    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECIPIENT_EMAIL

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER_EMAIL, APP_PASSWORD)
        server.send_message(msg)

def send_html_email(subject, html):
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECIPIENT_EMAIL
    msg.attach(MIMEText(html, "html"))

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER_EMAIL, APP_PASSWORD)
        server.send_message(msg)

# -------- Email 1 --------
send_text_email(
    "Peak hour approaching",
    "Peak hour is approaching. Please plan to shift your electricity consumption."
)
time.sleep(300)

# -------- Email 2 --------
send_text_email(
    "Peak load hours",
    "You are currently in peak load hours. Please click below to confirm load shift."
)
time.sleep(300)

# -------- Email 3 (buttons) --------
html_body = f"""
<html>
<body style="font-family: Arial;">
  <p><b>How much load did you shift today?</b></p>

  <a href="{BASE_URL}10"
     style="padding:10px;background:#4CAF50;color:white;text-decoration:none;border-radius:6px;margin-right:8px;">
     10%
  </a>

  <a href="{BASE_URL}25"
     style="padding:10px;background:#2196F3;color:white;text-decoration:none;border-radius:6px;margin-right:8px;">
     25%
  </a>

  <a href="{BASE_URL}50"
     style="padding:10px;background:#FF5722;color:white;text-decoration:none;border-radius:6px;">
     50%
  </a>
</body>
</html>
"""

send_html_email("Load shift feedback", html_body)
print("✅ Emails sent")
