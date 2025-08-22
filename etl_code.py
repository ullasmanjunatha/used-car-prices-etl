#import all the libraries

import glob
import pandas as pd 
import xml.etree.ElementTree as ET
from datetime import datetime
import json 

# Files to create
log_file = "log_progress.txt"
target_file = "transformed_data.csv"
extracted_data = pd.DataFrame()

# Extraction of data

#.csv file
def extract_from_csv(file_to_process):
    df = pd.read_csv(file_to_process)
    return df
#.json file
def extract_from_json(file_to_process):
    df = pd.read_json(file_to_process, lines=True)
    return df


#.xml file
def extract_from_xml(file_to_process):
    df = pd.DataFrame(columns=['car_model', 'year_of_manufacture', 'price', 'fuel'])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for row in root.findall('row'):
        car_model = row.find('car_model').text if row.find('car_model') is not None else None
        year_of_manufacture = row.find('year_of_manufacture').text if row.find('year_of_manufacture') is not None else None
        price = row.find('price').text if row.find('price') is not None else None
        fuel = row.find('fuel').text if row.find('fuel') is not None else None

        df = pd.concat([df, pd.DataFrame([{
            "car_model": car_model,
            "year_of_manufacture": year_of_manufacture,
            "price": price,
            "fuel": fuel
        }])], ignore_index=True)
    return df


def extract():
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])

    # Extract all csv file, except target_file
    for csvfile in glob.glob("*.csv"):
        if csvfile != target_file:
            extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    # Extract all json file
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    # Extract all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data

#-------------------------------------------------------------------------------------------------------------------

#transform the data

# convert price into numeric value
def transform(data):
    if data is None or data.empty:
        print("No data to transform")
        return pd.DataFrame()
    if "price" not in data.columns:
        print("Price column is missing")
        return data


    data["price"] = pd.to_numeric(data["price"], errors='coerce')
# converting Euros to inr , 1 Euro = 100inr
    data["price"] = round(data.price * 100,2)
    return data


#-----------------------------------------------------------------------------

#Load Data
def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)

#-------------------------------------------------------------------------------

#Log the progress

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:-%M:-%S:'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')

#---------------------------------------------------------------------------------

#Log Job started
log_progress("ETL job started")

#Extract process start
log_progress("Extract phase started")
extracted_data = extract()

#Extract process end
log_progress("Extract phase ended")

#Transform phase
log_progress("Transform phase started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

#transform phase end
log_progress("Transform phase ended")

#Load phase started
log_progress("Load phase started")
load_data(target_file, transformed_data)

#Load phase ended
log_progress("Load phase ended")

#ETL job ended
log_progress("ETL job ended")
