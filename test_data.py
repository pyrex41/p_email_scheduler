import pandas as pd
import json
import random
from datetime import datetime, timedelta
import faker
import argparse

# Set up argument parser
parser = argparse.ArgumentParser(description='Generate dummy insurance data from email list')
parser.add_argument('--input', '-i', required=True, help='Input CSV file containing email addresses')
parser.add_argument('--output', '-o', required=True, help='Output CSV file for generated data')
args = parser.parse_args()

# Initialize Faker for generating realistic names and phone numbers
fake = faker.Faker()

# Load ZIP code data
with open('zipData.json', 'r') as f:
    zip_data = json.load(f)
zip_codes = list(zip_data.keys())

# Load email addresses from input file
df = pd.read_csv(args.input)
emails = df.iloc[:, 1].dropna().tolist()  # Assuming email is in second column

# Insurance carriers
carriers = ['Humana', 'UHC', 'Aetna', 'Cigna', 'Anthem', 'United Healthcare']

# Generate all data at once using pandas
num_records = len(emails)

# Generate date ranges
today = datetime.now()
ten_years_ago = today - timedelta(days=365*10)

# Create lists of dates for sampling
effective_dates = pd.date_range(start=ten_years_ago, end=today, freq='MS').strftime('%Y-%m-%d').tolist()
birth_dates = pd.date_range(start='1940-01-01', end='1958-12-31', freq='D').strftime('%Y-%m-%d').tolist()

# Create DataFrame with all random data
df = pd.DataFrame({
    'First Name': [fake.first_name() for _ in range(num_records)],
    'Last Name': [fake.last_name() for _ in range(num_records)],
    'Email': emails,
    'Current Carrier': random.choices(carriers, k=num_records),
    'Plan Type': random.choices(['N', 'G'], k=num_records),
    'Effective Date': random.choices(effective_dates, k=num_records),
    'Birth Date': random.choices(birth_dates, k=num_records),
    'Tobacco User': random.choices(['Yes', 'No'], k=num_records),
    'Gender': random.choices(['M', 'F'], k=num_records),
    'ZIP Code': random.choices(zip_codes, k=num_records),
    'Phone Number': [
        f'({random.randint(100,999)}) {random.randint(100,999)}-{random.randint(1000,9999)}'
        for _ in range(num_records)
    ]
})

# Reorder columns to match desired output
df = df[['First Name', 'Last Name', 'Email', 'Current Carrier', 'Plan Type', 
         'Effective Date', 'Birth Date', 'Tobacco User', 'Gender', 'ZIP Code', 
         'Phone Number']]

# Write to CSV
df.to_csv(args.output, index=False)
print(f"Generated data has been written to {args.output}")