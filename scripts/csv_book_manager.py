#!/usr/bin/env python3

#########################################################
# Importing libraries
#########################################################

import platform
import os
import time
import sys
import logging
import datetime
import os
import pandas as pd
import pandera.pandas as pa
from logging import handlers
from pandera import Column, Check

#########################################################
# Basic script config
#########################################################

FILE_ENCODING = "utf-8"
CURRENT_OS = platform.platform()
FILE_PERMISSION = "r"
CHECKING_DIRECTORY = "../books"

# Logging config

original_emit = logging.StreamHandler.emit

def colored_emit(self, record):
    # ANSI color codes
    COLORS = {
        logging.DEBUG: '\033[36m',    # Cyan
        logging.INFO: '\033[32m',     # Green
        logging.WARNING: '\033[33m',  # Yellow
        logging.ERROR: '\033[31m',    # Red
        logging.CRITICAL: '\033[1;41m' # Bold Red on white background
    }
    RESET = '\033[0m'
    
    # Add color to the record
    if self.stream.isatty():  # Only color if it's a terminal
        color = COLORS.get(record.levelno, '')
        record.levelname = f"{color}{record.levelname}{RESET}"
        record.msg = f"{color}{record.msg}{RESET}"
    
    # Call original emit
    original_emit(self, record)

logging.StreamHandler.emit = colored_emit

logging.basicConfig(
    level=logging.INFO,
    datefmt="%d/%m/%Y %H:%M:%S",
    format="[%(asctime)s] {%(pathname)s} %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(stream=sys.stdout),
        handlers.RotatingFileHandler(
            "../tmp/tmp.log",
            mode="a",
            maxBytes=104857600,
            backupCount=2,
            encoding="utf-8"
        )
    ]
)

logger = logging.getLogger(__name__)

#########################################################
# Application Banner
#########################################################

def bannerPrint():
    try:
        print("\n========================\n")
        print(f"File-Encoding: {FILE_ENCODING}")
        print(f"Current-Os: {CURRENT_OS}")
        print(f"File-Permission: {FILE_PERMISSION}")
        print(f"Examination-Dir: {CHECKING_DIRECTORY.split(sep="/")[1]}")
        print("\n========================\n")
    except:
        raise Exception("There was an error while launching script")

#########################################################
# Csv driver code
#########################################################

################
# Determining Csv types and fields
################

book_schema = pa.DataFrameSchema(
    {
        "ID": Column(str, checks=Check.str_length(min_value=1), nullable=False, unique=True),
        "Title": Column(str, checks=Check.str_length(min_value=1), nullable=False, unique=True),
        "Author": Column(str, nullable=True),
        "Year": Column(str, checks=Check.str_matches(r'^\d{4}$|^$'), nullable=True), # 4 digits or empty
        "Category": Column(str, checks=Check.isin(["CS", "CE", "PHYS", "MECH", "MATH", "AI", "DRONE"]), nullable=False),
        "Subcategory": Column(str, checks=Check.str_length(min_value=1), nullable=False),
        "Status": Column(str, checks=Check.isin(["to-read", "reading", "completed", "reference"]), nullable=False),
        "Rating": Column(str, checks=Check.str_matches(r'^[0-5](\.[0-9])?$|^$'), nullable=False), # e.g., 4.2
        "Notes": Column(str, nullable=True),
        "ISBN": Column(str, nullable=True),
        "Format": Column(str, checks=Check.isin(["hardcover", "pdf", "ebook"]), nullable=False),
    },
    strict=True,  # No extra columns allowed
    coerce=True   # Try to convert data to the specified string type
)

null_values = [
    "NA",
    "N/A",
    "missing",
    "null",
    "-",
    "",
    "NaN"
]

columns=[
    "ID",
    "Title",
    "Author",
    "Year",
    "Category",
    "Subcategory",
    "Status",
    "Rating",
    "Notes",
    "ISBN",
    "Format"
]

data_type = {
    "ID": "object",
    "Title": "object",
    "Author": "object",
    "Year": "object",
    "Category": "object",
    "Subcategory": "object",
    "Status": "object",
    "Rating": "object",
    "Notes": "object",
    "ISBN": "object",
    "Format": "object"
}

# Finding Csv files

## Todo: Add typing for managing id

def find_file(filename: str, search_path: str):
    for root, dirs, files in os.walk(search_path):
        if filename in files:
            return os.path.join(root, filename)
    return None # File not found

def mainFunc(fileName: str) -> pd.DataFrame:
    try:
        found_path = find_file(fileName, CHECKING_DIRECTORY)
        if not found_path:
            logging.error(f"File {fileName} not found in {CHECKING_DIRECTORY}, Exiting...")
            sys.exit(1)
        df = pd.read_csv(found_path, na_values=null_values)
        logging.info(f"File {fileName} found and loaded. Validating schema...")
        validated_df = book_schema.validate(df, lazy=True)
        logging.info("CSV schema validation passed.")
        return validated_df
    except Exception as e:
        raise Exception("An error occured")

def searchFunc(search_item: str, df: pd.DataFrame):
    try:
        search_result = df[df["Title"].str.contains(search_item, case=False, na=False)]
        if search_result.empty:
            logging.info(f"No records found for {search_item}.")
            return None
        else:
            logging.info(f"Found {len(search_result)} record(s) for {search_item}.")
            return search_result
    except Exception as e:
        raise Exception("An error occured")

def add_record(
        ID: str,
        Title: str,
        Author: str,
        Year: str,
        Category: str,
        Subcategory: str,
        Status: str,
        Rating: str,
        Notes: str,
        ISBN: str,
        Format: str,
        FilePath: str
):
    try:
        #new_record = pd.DataFrame([[ID, Title, Author, Year, Category, Subcategory, Status, Rating, Notes, ISBN, Format]], columns=columns)
        #new_record.to_csv(FilePath, mode="a", header=False, index=False)
        new_record_df = pd.DataFrame([{
            "ID": ID, "Title": Title, "Author": Author, "Year": Year,
            "Category": Category, "Subcategory": Subcategory, "Status": Status,
            "Rating": Rating, "Notes": Notes, "ISBN": ISBN, "Format": Format
        }])

        # 2. Validate this single record against the schema
        book_schema.validate(new_record_df, lazy=True)
        logging.info("New record validation passed.")

        # 3. Append to CSV (mode='a' writes headers only if file doesn't exist)
        new_record_df.to_csv(FilePath, mode="a", header=False, index=False, lineterminator='\n', encoding="utf-8")
        logging.info("Record successfully appended to file.")
    except Exception as e:
        print(e)
        raise Exception("An error occured")

#########################################################
# Main Entry point
#########################################################

if __name__ == "__main__":
    try:

        ##############################################
        # Printing banner and additional infos
        ##############################################

        bannerPrint()
        
        ##########################
        # Fetching files
        ##########################

        logging.info("Fetching csv list...", )
        
        listOfFiles = os.listdir(CHECKING_DIRECTORY)
        csvFileCount: list[str] = []
        for files in listOfFiles:
            if files.endswith(".csv"):
                csvFileCount.append(files)
            else:
                continue

        # If there isn't any csv file, then exit program

        if len(csvFileCount) == 0:
            logging.error("There is no file, Exiting...")
            sys.exit(0)

        logging.info(f"Founded {len(csvFileCount)} csv files.")
        getUserFileInput = str(input("Enter your file name: "))
        
        csvFileIndex = csvFileCount.index(getUserFileInput)
        getFile = find_file(csvFileCount[csvFileIndex], CHECKING_DIRECTORY)
        
        if getFile is None:
            logging.error("Could not find file, Exiting...")
            sys.exit(1)
        
        validateFile = mainFunc(getUserFileInput)
        #getFileId = validateFile.tail(1)["ID"].tolist()[0]

        logging.info('Categories are "CS", "CE", "PHYS", "MECH", "MATH", "AI", "DRONE"')
        logging.info('Status are "to-read", "reading", "completed", "reference"')
        getUserFileInputChoice = str(input("Choise between bulk or single mode: (b for bulk, s for single) "))
        data_tmp = {
            "ID": "object",
            "Title": "object",
            "Author": "object",
            "Year": "object",
            "Category": "object",
            "Subcategory": "object",
            "Status": "object",
            "Rating": "object",
            "Notes": "object",
            "ISBN": "object",
            "Format": "object"
        }
        if getUserFileInputChoice == "s":
            idx = 0
            while True:
                if idx == len(columns):
                    logging.info("Adding is finished")
                    break
                getUserInput = str(input(f"Enter {columns[idx]}: "))
                data_tmp[columns[idx]] = getUserInput
                idx += 1
            add_record(
                data_tmp["ID"],
                data_tmp["Title"],
                data_tmp["Author"],
                data_tmp["Year"],
                data_tmp["Category"],
                data_tmp["Subcategory"],
                data_tmp["Status"],
                data_tmp["Rating"],
                data_tmp["Notes"],
                data_tmp["ISBN"],
                data_tmp["Format"],
                getFile
            )
            sys.exit(1)
        if getUserFileInputChoice == "b":
            idx = 0
            while True:
                while True:
                    if idx == len(columns):
                        logging.info("Adding is finished")
                        break
                    getUserInput = str(input(f"Enter {columns[idx]}: "))
                    data_tmp[columns[idx]] = getUserInput
                    idx += 1
                add_record(
                    data_tmp["ID"],
                    data_tmp["Title"],
                    data_tmp["Author"],
                    data_tmp["Year"],
                    data_tmp["Category"],
                    data_tmp["Subcategory"],
                    data_tmp["Status"],
                    data_tmp["Rating"],
                    data_tmp["Notes"],
                    data_tmp["ISBN"],
                    data_tmp["Format"],
                    getFile
                )
                isContinue = str(input("Do you wish to continue? (y for yes, n for no)"))
                if isContinue == "n":
                    logging.info("Adding is finished")
                    sys.exit(1)
        logging.error("Error, Exiting...")
        sys.exit(1)

    except Exception as e:
        logging.error("Exception ocured")
        sys.exit(1)