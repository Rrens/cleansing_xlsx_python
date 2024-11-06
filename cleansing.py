import pandas as pd
import re

def is_valid_email(email):
    email_regex = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"
    return re.match(email_regex, email) is not None

input_file = 'source.xlsx'
output_file = 'output_cleaned.csv'

df = pd.read_excel(input_file)

df = df.dropna(subset=['Nama', 'Email', 'Nomor Telephone', 'SMA/SMK/MA'])

df['email_valid'] = df['Email'].apply(is_valid_email)
df = df[df['email_valid']]

df = df.drop(columns=['email_valid'])

df.to_csv(output_file, index=False)

print(f"Data yang bersih uda ada disini {output_file}")
