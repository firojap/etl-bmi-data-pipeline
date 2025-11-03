# import all dependencies
import glob
import os
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

# ------------------------------------------------------------
# DEFINE PATHS
# ------------------------------------------------------------
BASE_DIR = r"C:\Users\firoj\OneDrive\Desktop\IBM_DE\ETL_BMI_data"
SOURCE_DIR = os.path.join(BASE_DIR, "source")
TARGET_DIR = os.path.join(BASE_DIR, "Target")

# create target folder if it doesn’t exist
os.makedirs(TARGET_DIR, exist_ok=True)

# define file paths
log_file = os.path.join(TARGET_DIR, "log_file.txt")
target_file = os.path.join(TARGET_DIR, "transformed_data.csv")

# ------------------------------------------------------------
# EXTRACTION FUNCTIONS
# ------------------------------------------------------------
def extract_from_csv(file_to_process):
    return pd.read_csv(file_to_process)

def extract_from_json(file_to_process):
    return pd.read_json(file_to_process, lines=True)

def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat(
            [dataframe, pd.DataFrame([{"name": name, "height": height, "weight": weight}])],
            ignore_index=True
        )
    return dataframe

# ------------------------------------------------------------
# EXTRACT
# ------------------------------------------------------------
def extract():
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])

    # process all csv files
    for csvfile in glob.glob(os.path.join(SOURCE_DIR, "*.csv")):
        extracted_data = pd.concat(
            [extracted_data, extract_from_csv(csvfile)], ignore_index=True
        )

    # process all json files
    for jsonfile in glob.glob(os.path.join(SOURCE_DIR, "*.json")):
        extracted_data = pd.concat(
            [extracted_data, extract_from_json(jsonfile)], ignore_index=True
        )

    # process all xml files
    for xmlfile in glob.glob(os.path.join(SOURCE_DIR, "*.xml")):
        extracted_data = pd.concat(
            [extracted_data, extract_from_xml(xmlfile)], ignore_index=True
        )

    return extracted_data

# ------------------------------------------------------------
# TRANSFORM
# ------------------------------------------------------------
def transform(data):
    # ensure numeric types
    data["height"] = pd.to_numeric(data["height"], errors="coerce")
    data["weight"] = pd.to_numeric(data["weight"], errors="coerce")

    # convert and round
    data["height"] = (data["height"] * 0.0254).round(2)
    data["weight"] = (data["weight"] * 0.45359237).round(2)
    return data

# ------------------------------------------------------------
# LOAD
# ------------------------------------------------------------
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file, index=False)

# ------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------
def log_progress(message):
    timestamp_format = "%Y-%m-%d %H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(f"{timestamp}, {message}\n")

# ------------------------------------------------------------
# MAIN ETL PROCESS
# ------------------------------------------------------------
log_progress("ETL Job Started")

log_progress("Extract phase Started")
extracted_data = extract()
log_progress("Extract phase Ended")

log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data:")
print(transformed_data)
log_progress("Transform phase Ended")

log_progress("Load phase Started")
load_data(target_file, transformed_data)
log_progress("Load phase Ended")

log_progress("ETL Job Ended")
