import sqlite3
import pandas as pd
import smtplib
from email.message import EmailMessage

DB_FILE = "privateproperty.db"

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
SMTP_USER = "your_email@gmail.com"
SMTP_PASS = "your_app_password"
RECIPIENTS = ["you@example.com"]

def generate_summary():
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("""
        SELECT area,
               COUNT(*) AS listings,
               ROUND(AVG(price_zar)) AS avg_rent
        FROM listings
        GROUP BY area
    """, conn)
    conn.close()

    df.to_csv("weekly_summary.csv", index=False)
    return "weekly_summary.csv"

def send_email(attachment):
    msg = EmailMessage()
    msg["Subject"] = "Weekly Rental Market Summary"
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(RECIPIENTS)
    msg.set_content("Attached is the latest rental market summary.")

    with open(attachment, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="text",
            subtype="csv",
            filename=attachment
        )

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
        s.starttls()
        s.login(SMTP_USER, SMTP_PASS)
        s.send_message(msg)

if __name__ == "__main__":
    send_email(generate_summary())
