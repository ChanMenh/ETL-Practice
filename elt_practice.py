import glob  # Used to find files matching a pattern
import pandas as pd  # For data manipulation
import xml.etree.ElementTree as ET  # For parsing XML files
from datetime import datetime  # For logging timestamps

# Log file to record ETL progress
log_file = "log_file.txt"

# Output CSV file where transformed data will be stored
target_file = "transformed_data.csv"

# ----------- Extraction Functions -----------

# Extract data from CSV file using pandas
def extract_from_csv(file):
    return pd.read_csv(file)

# Extract data from JSON file using pandas
def extract_from_json(file):
    return pd.read_json(file, lines=True)

# Extract data from XML file using ElementTree and construct a DataFrame
def extract_from_xml(file):
    dataframe = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])  # Create empty DataFrame
    tree = ET.parse(file)  # Parse XML file
    root = tree.getroot()  # Get root element

    # Loop through each car record in the XML
    for model in root:
        car_model = model.find("car_model").text
        year_of_manufacture = float(model.find("year_of_manufacture").text)
        price = float(model.find("price").text)
        fuel = model.find("fuel").text
        row = {"car_model": car_model, "year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}
        
        # Append the row to the DataFrame
        dataframe = pd.concat([dataframe, pd.DataFrame([row])], ignore_index=True)

    return dataframe

# ----------- Main Extraction Logic -----------

# Combine data from all CSV, JSON, and XML files into a single DataFrame
def extract():
    extracted_data = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])  # Initialize empty DataFrame

    # Process all CSV files (excluding the target output file)
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            extracted_data = pd.concat([extracted_data, extract_from_csv(csvfile)], ignore_index=True)

    # Process all JSON files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, extract_from_json(jsonfile)], ignore_index=True)

    # Process all XML files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, extract_from_xml(xmlfile)], ignore_index=True)

    return extracted_data

# ----------- Transformation Function -----------

# Convert price column to numeric and round to 2 decimal places
def transform(data):
    data['price'] = pd.to_numeric(data['price'], errors='coerce')  # Convert price to numeric, invalid values become NaN
    data['price'] = data['price'].round(2)  # Round to 2 decimal places
    
    data['year_of_manufacture'] = data['year_of_manufacture'].astype(int)
    return data

# ----------- Load Function -----------

# Save transformed data to a CSV file
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

# ----------- Logging Function -----------

# Record ETL progress messages with a timestamp in the log file
def log_progress(message):
    timestamp_format = '%Y-%b-%d-%H:%M:%S'  # Format: e.g., 2025-Jun-07-15:30:59
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp},{message}\n")

# ----------- ETL Process Execution -----------

log_progress("ETL Job Started")  # Start log

log_progress("Extract phase Started")
extracted_data = extract()  # Run extraction
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
transformed_data = transform(extracted_data)  # Run transformation
print("Transformed Data")
print(transformed_data)  # Display transformed data in console
log_progress("Transform phase Ended")

log_progress("Load phase Started")
load_data(target_file, transformed_data)  # Save transformed data
log_progress("Load phase Ended")

log_progress("ETL Job Ended")  # End log
