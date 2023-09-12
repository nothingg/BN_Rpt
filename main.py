import os
import pandas as pd
# import psycopg2
from sqlalchemy import create_engine
from datetime import datetime


class CSVToPostgreSQL:

  def __init__(self, csv_directory, db_params):
    self.csv_directory = csv_directory
    self.db_params = db_params
    self.column_names = [
        'cs_ref', 'instruction_id', 'mt', 'ctgypurp', 'dr_bic', 'dr_acct',
        'cr_bic', 'cr_acct', 'dr_amt', 'cr_amt', 'status', 'error', 'time',
        'ch', 'transmission_type', 'debtor_acct', 'debtor_name',
        'creditor_acct', 'creditor_name'
    ]
    self.excel_column_names = [
        'row_no', 'transfer_ref_no', 'transfer_date_buddist', 'to_bank', 'amt',
        'department'
    ]

  def readexcel(self):
    # Read the Excel file
    excel_file = "assets/excel/bahtnet_running_no.xlsx"
    data = pd.read_excel(excel_file,
                         sheet_name='Sheet1',
                         header=0,
                         #skiprows=900,
                         #nrows=3600,
                         usecols='A:F',
                         dtype={'transfer_ref_no': str },
                         names=self.excel_column_names)

    #1022 - row 1024
    data = data.dropna(subset=['department'])
    data['transfer_ref_no'] = data['transfer_ref_no'].str.replace(" ","",regex=True)
    data['transfer_date_christian'] = data['transfer_date_buddist'].apply(self.convert_buddhist_to_gregorian)

    return data

  def convert_buddhist_to_gregorian(self, buddhist_date):

      buddhist_date = pd.Series(buddhist_date)
      year = int(buddhist_date.str.slice(start=-4, stop=None).iloc[0])
      month = int(buddhist_date.str.slice(start=-7, stop=-5).iloc[0])
      day = int(buddhist_date.str.slice(start=0, stop=2).iloc[0])

      year_gregorian = year - 543

      # Create a new date string in the Gregorian format 'YYYY-mm-dd'
      gregorian_date = f"{year_gregorian:04d}-{month:02d}-{day:02d}"

      return gregorian_date

  def insert_data_excel(self, data):
    engine = create_engine(self.db_params)

    data.to_sql('bahtnet_no', engine, if_exists='replace', index=False)

  def read_csv_files(self):
    combined_data = pd.DataFrame(columns=self.column_names)

    for filename in os.listdir(self.csv_directory):
      if filename.endswith('.csv'):
        file_path = os.path.join(self.csv_directory, filename)

        data = pd.read_csv(file_path, skiprows=18, names=self.column_names)
        combined_data = pd.concat([combined_data, data], ignore_index=True)

    return combined_data

  def insert_into_postgresql(self, data):
    engine = create_engine(self.db_params)

    data.to_sql('reports', engine, if_exists='replace', index=False)

  def rearrange_csv_data(self, combined_data):
    # combined_data['cr_amt'] = combined_data['cr_amt'].str.replace(",", "").fillna(0).astype(float)
    combined_data['cr_amt'] = combined_data['cr_amt'].str.replace(
        ",", "").astype(float)

    combined_data['dr_amt'] = combined_data['dr_amt'].str.replace(
        ",", "").astype(float)

    # remove single quotes from the first position of a column
    combined_data['instruction_id'] = combined_data[
        'instruction_id'].str.replace("^'", "", regex=True)
    combined_data['debtor_acct'] = combined_data['debtor_acct'].str.replace(
        "^'", "", regex=True)
    combined_data['creditor_acct'] = combined_data[
        'creditor_acct'].str.replace("^'", "", regex=True)

    # Convert the 'transactions_time' column to string
    combined_data['transactions_time'] = combined_data['cs_ref'].astype(str)

    # Extract the first 6 characters as a string
    combined_data['date_string'] = combined_data['transactions_time'].str[:6]

    # Convert the date string to datetime format
    combined_data['report_date'] = pd.to_datetime(combined_data['date_string'],
                                                  format='%y%m%d',
                                                  errors='coerce')

    # Drop the intermediate 'date_string' column if you don't need it
    combined_data = combined_data.drop(columns=['date_string'])

    return combined_data




if __name__ == "__main__":
  # csv_directory = '/home/runner/BahtnetRptpd/assets/csv/'
  # excel_directory = '/home/runner/BahtnetRptpd/assets/excel/'

  csv_directory = '/assets/csv/'
  excel_directory = '/assets/excel/'

  db_params = "postgresql://gpjaulxp:t33MItntW9YX3h6GD8fePdUu-DgtI6Vh@rain.db.elephantsql.com/gpjaulxp"

  csv_to_postgresql = CSVToPostgreSQL(csv_directory, db_params)
  # combined_data = csv_to_postgresql.read_csv_files()
  # combined_data = csv_to_postgresql.rearrange_csv_data(combined_data)
  # csv_to_postgresql.insert_into_postgresql(combined_data)

  excel_data = csv_to_postgresql.readexcel()



  print(excel_data)
  # print(f"{excel_data['transfer_date_christian']} -- {excel_data['transfer_date_buddist']}" )
  # csv_to_postgresql.insert_data_excel(excel_data)

  # file_path = 'output.xlsx'
  # combined_data.to_excel(file_path, index=False)
