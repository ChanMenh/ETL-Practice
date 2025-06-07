import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file):
    return pd.read_csv(file)

def extract_from_json(file):
    return pd.read_json(file, lines=True)

def extract_from_xml(file):
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file)
    root = tree.getroot()
    for model in root:
        car_model = model.find("car_model").text
        year_of_manufacture = float(model.find("year_of_manufacture").text)
        price = float(model.find("price").text)
        fuel = model.find("fuel").text
        row = {"car_model": car_model, "year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}
        dataframe = pd.concat([dataframe, pd.DataFrame([row])], ignore_index=True)
    return dataframe

def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])

    # CSV files
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    # JSON files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    # XML files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)

    return extracted_data

def transform(data):
    '''Convert price column to numeric and round to two decimals'''
    data['price'] = pd.to_numeric(data['price'], errors='coerce')
    data['price'] = data['price'].round(2)
    return data

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  # Corrected format: e.g. 2025-Jun-07-15:30:59
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp},{message}\n")

# ETL process execution
log_progress("ETL Job Started")

log_progress("Extract phase Started")
extracted_data = extract()
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)
log_progress("Transform phase Ended")

log_progress("Load phase Started")
load_data(target_file, transformed_data)
log_progress("Load phase Ended")

log_progress("ETL Job Ended")
