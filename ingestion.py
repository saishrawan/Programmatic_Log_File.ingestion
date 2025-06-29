import subprocess
import pyodbc
import zipfile
import os
import csv
from datetime import datetime, timedelta
import logging
from logging.handlers import RotatingFileHandler
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formatdate, make_msgid
import pandas as pd

# Get the current date and time
current_time = datetime.now()

# Format the timestamp.
timestamp = current_time.strftime('%Y-%m-%d_%H-%M-%S')

# Get the current date and time and subtract one day for yesterday
yesterday = datetime.now() - timedelta(days=1)

# Format the timestamp for file naming and the date for URL
timestamp_yesterday = yesterday.strftime('%Y-%m-%d_%H-%M-%S')
formatted_date = yesterday.strftime('%Y-%m-%d')

current_date = datetime.now().strftime("%Y-%m-%d")

# "F:\Output\file_{timestamp}"
# Setup logging
log_file_path = f"F:\Logs\clicks_log_{current_date}.log"
logging.basicConfig(handlers=[RotatingFileHandler(log_file_path, maxBytes=1000000, backupCount=7)],
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)

"""
from_email = "pocnnotifications@gmail.com"
email_password = "gtza bkcz icen fxhb"
smtp_server = "smtp.gmail.com"
to_emails = ["svutharkar@pocn.com", "lcarrete@pocn.com", "tgreble@pocn.com"]

def send_notification_email(subject, body, port=587):
    try:
        # Set up the MIME message
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = ", ".join(to_emails)  # Join all recipients in the to_emails list with a comma
        msg['Subject'] = subject

        # Add body to email
        msg.attach(MIMEText(body, 'plain'))

        # Set up the SMTP server
        server = smtplib.SMTP(smtp_server, port)
        server.starttls()  # Enable security
        server.login(from_email, email_password)  # Log in to your email account
        text = msg.as_string()
        server.sendmail(from_email, to_emails, text)  # Send the email to all recipients in the to_emails list
        server.quit()
        logging.info("Email sent successfully!")
    except Exception as e:
        logging.info(f"Failed to send email: {e}")
"""

# ── Configuration ──────────────────────────────────────────────────────────────
from_email     = "pocnnotifications@gmail.com"
email_password = "XX"
smtp_server    = "smtp.gmail.com"
to_emails      = ["XXX", "XXX", "XXX"]

def send_notification_email(subject, body, port=587):
    # Build the message with the proper headers
    msg = MIMEMultipart()
    msg['From']    = from_email
    msg['To']      = ", ".join(to_emails)
    msg['Subject'] = subject
    
    # Best‑practice headers to boost deliverability
    msg['Date']         = formatdate(localtime=True)
    msg['Message-ID']   = make_msgid(domain=from_email.split('@')[-1])
    msg['List-Unsubscribe'] = f"<mailto:alerts-unsubscribe@{from_email.split('@')[-1]}>"
    
    msg.attach(MIMEText(body, 'plain'))
    
    try:
        # Use a context manager to ensure the connection always closes
        with smtplib.SMTP(smtp_server, port, timeout=30) as server:
            server.set_debuglevel(1)   # log every SMTP command/response to stdout
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(from_email, email_password)
            server.sendmail(from_email, to_emails, msg.as_string())
        
        logging.info("Email sent successfully!")
    except Exception as e:
        # log full stack trace for easier debugging
        logging.error(f"Failed to send email: {e}", exc_info=True)
        
def download_log_file(url, output_path, username, password):
    try:
        command = f'curl -u {username}:{password} -o "{output_path}" "{url}"'
        subprocess.run(command, check=True, shell=True)
        logging.info(f"Downloaded log file to {output_path}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to download log file: {e}")

def extract_zip(zip_file_path, extract_to):
    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
        logging.info(f"Extracted files to {extract_to}")

def read_csv_with_encoding(file_path, encodings=['utf-8', 'iso-8859-1', 'latin1', 'cp1252']):
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            logging.info(f"Successfully read the file with {encoding} encoding")
            return encoding
        except UnicodeDecodeError as e:
            logging.info(f"Failed to read with {encoding} encoding: {e}")
    raise ValueError("Unable to read the CSV file with the provided encodings")

def upload_to_sql_azure(filename, file_path, server, database, username, password, table_name, category, encoding):
    conn = None
    cursor = None
    try:
        conn_str = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=Yes'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        logging.info("Connected to SQL Azure successfully.")

        with open(file_path, 'r', newline='', encoding=encoding) as csvfile:
            csvreader = csv.reader(csvfile)
            header = next(csvreader)  # Read the header row
            if not header:
                raise ValueError("CSV file does not contain a header row.")
                
            placeholders = ', '.join(['?'] * len(header))  # Create placeholders for SQL query
            sql_query = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({placeholders})"
            
            for row in csvreader:
                cursor.execute(sql_query, row)
            conn.commit()
            logging.info(f"{filename} Data uploaded to SQL Azure {table_name} table successfully.")
            email_subject = f"Uploaded {category} Data File"
            email_body = f"Uploaded {filename} data to SQL Azure {table_name} successfully"
            send_notification_email(email_subject, email_body)
    except Exception as e:
        error_type = type(e).__name__
        error_message = f"An error occurred: {error_type}, {e} Failed to upload {filename} data to SQL Azure {table_name}"
        logging.error(error_message)
        # Send email notification
        email_subject = f"Error Uploading {category} Data File"
        email_body = f"An error occurred while uploading {filename} data to SQL Azure {table_name}. Error Details: {error_message}. Check Log Files"
        send_notification_email(email_subject, email_body)
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Configuration
log_file_url = f"https://statsfiles.wowcon.net/rtb140/clicks-rtb140-{formatted_date}.csv.zip"
#log_file_url = f"https://statsfiles.wowcon.net/rtb140/clicks-rtb140-2025-06-10.csv.zip"
username = 'XXX'
password = 'XXX'
zip_output_path = rf"F:\Output\file_{timestamp}.csv.zip"
extract_to = rf"F:\Output\file_{timestamp}"  
sql_server = 'XXX'
sql_database = "XXX"
sql_username = "XXX"
sql_password = "XXX"

impressions_log_file_url = f"https://statsfiles.wowcon.net/rtb140/impressions-rtb140-{formatted_date}.csv.zip"
impressions_zip_output_path = rf"F:\Output\impressions_file_{timestamp}.csv.zip"
impressions_extract_to = rf"F:\Output\impressions_file_{timestamp}"

# Process Execution
download_log_file(log_file_url, zip_output_path, username, password)
extract_zip(zip_output_path, extract_to)

table_name = 'AdKernelProgrammaticDataClicks'

# Assuming there might be multiple CSV files in the ZIP
for filename in os.listdir(extract_to):
    category = 'Clicks'
    if filename.endswith(".csv"):
        csv_file_path = os.path.join(extract_to, filename)
        encoding = read_csv_with_encoding(csv_file_path)
        upload_to_sql_azure(filename, csv_file_path, sql_server, sql_database, sql_username, sql_password,table_name, category, encoding)
