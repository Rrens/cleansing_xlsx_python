import pandas as pd
import re
import logging
from smtplib import SMTP, SMTPException
import dns.resolver
import smtplib

logging.basicConfig(
    filename='email_check.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def is_valid_email(email):
    email_regex = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
    return re.match(email_regex, email) is not None


def cleansing():
    input_file = 'source.xlsx'
    output_file = 'output_cleaned.xlsx'

    df = pd.read_excel(input_file)

    df = df.dropna(subset=['Nama', 'Email', 'Nomor Telephone', 'SMA/SMK/MA'])

    df['email_valid'] = df['Email'].apply(is_valid_email)
    df = df[df['email_valid']]

    df = df.drop(columns=['email_valid'])
    
    check_email(df)

    print(f"Data yang bersih sudah ada di sini: {output_file}")

def get_mx_record(domain):
    try:
        logging.info(f"Mencari MX record untuk domain: {domain}")
        answers = dns.resolver.resolve(domain, 'MX')
        mx_record = answers[0].exchange.to_text()
        logging.info(f"MX record ditemukan untuk {domain}: {mx_record}")
        return mx_record
    except dns.resolver.NoAnswer:
        logging.warning(f"Tidak ada jawaban MX record untuk domain: {domain}")
        return None
    except dns.resolver.NXDOMAIN:
        logging.warning(f"Domain tidak ditemukan: {domain}")
        return None
    except dns.resolver.Timeout:
        logging.error(f"Timeout saat mencari MX record untuk domain: {domain}")
        return None
    except Exception as e:
        logging.error(f"Error saat mencari MX record untuk {domain}: {e}")
        return None

def is_valid_email_format(email):
    email_regex = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
    return re.match(email_regex, email) is not None

def is_email_active(email):
    domain = email.split('@')[-1]
    mx_record = get_mx_record(domain)
    if not mx_record:
        logging.warning(f"MX record tidak ditemukan untuk domain: {domain}. Melewati email: {email}")
        return False
    
    try:
        logging.info(f"Mengecek email: {email} menggunakan MX record: {mx_record}")
        with smtplib.SMTP(mx_record, timeout=10) as smtp:
            smtp.ehlo_or_helo_if_needed()
            smtp.mail('me@rrens.me')
            code, response = smtp.rcpt(email)
            if code == 250:
                logging.info(f"Email aktif: {email}")
                return True
            else:
                logging.warning(f"Email tidak aktif: {email} (Kode: {code}, Respon: {response})")
                return False
    except smtplib.SMTPException as e:
        logging.error(f"Kesalahan SMTP saat mengecek email {email}: {e}")
        return False
    except Exception as e:
        logging.error(f"Gagal terhubung untuk email {email}: {e}")
        return False
    
def check_email(source):
    output_file = 'output_cleaned.xlsx'

    df = source
    logging.info("File loaded successfully")

    df = df.dropna(subset=['Nama', 'Email', 'Nomor Telephone', 'SMA/SMK/MA'])
    logging.info("Rows with empty required fields dropped")

    df['email_format_valid'] = df['Email'].apply(is_valid_email_format)

    df = df[df['email_format_valid']]
    logging.info("Rows with invalid email formats dropped")

    df['email_active'] = df['Email'].apply(is_email_active)

    df = df[df['email_active']]
    logging.info("Inactive emails removed")

    df = df.drop(columns=['email_format_valid', 'email_active'])

    df.to_excel(output_file, index=True)
    print(f"Data yang bersih sudah ada di sini: {output_file}")

cleansing()
