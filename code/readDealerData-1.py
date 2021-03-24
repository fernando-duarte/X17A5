"""
Archived Dealer Data Import from 1993

This function parses in dealer information with accompaning CIK codes for EDGAR lookup from the 
SEC dealer registration. All information is stored as a JSON object, with accompanying meta data

@author: Rajesh Rao (Sr. Research Analyst 22')
"""

# ----------------------------------------------------------
# Package Imports
# ----------------------------------------------------------

# console and directory access
import os
import re
import datetime

# interacting with Amazon AWS
import boto3
from sagemaker.session import Session

# data reading and exporting  
import json
import pandas as pd
import numpy as np

# parsing SEC website for data  
import requests 
from bs4 import BeautifulSoup

# ----------------------------------------------------------
# Function declarations
# ----------------------------------------------------------

def companyName(cik:str) -> str:
    """
    Returns the company name for a given CIK number from the SEC by parsing 
    the Edgar site for current company name filing
    
    Input:
        :param: cik (type str)
            The CIK number for a broker dealer e.g. 887767
    Return:
        :param: (type str)
            Returns the accompanying name with the CIK provided 
            e.g. 1ST GLOBAL CAPITAL CORP. 
    """
    
    # establishing base-url for company name search
    baseURL = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&'
    current_year = datetime.datetime.today().year
    url = baseURL+'CIK={}&type=X-17A-5&dateb={}1231'.format(cik, current_year)
    
    # response time for retrieving company names, returning beautifulsoup obj.
    res = requests.get(url, allow_redirects=True)
    s1 = BeautifulSoup(res.text, 'html.parser')
    
    # select the company information from the SEC website for a particular CIK
    for val in s1.find_all('span', attrs={"class":"companyName"}):
        
        # retrieve the company name from info class
        return val.text.split('CIK')[0].split('/BD')[0]


def dealerData(years:list, quarters:list=['QTR1', 'QTR2', 'QTR3', 'QTR4'], 
               cik2brokers:dict={'years-covered':[], 'broker-dealers':{}}) -> dict:
    """
    Retrieve dealer data from archived SEC directory, returns a dictionary 
    of essential information to be stored as a JSON file
    
    Input:
        :param: years (type list)
            A list of years to check for additional dealer data to be pulled e.g. [1993, 1994, 
            2000]. NOTE, that only the years specified are checked for dealer information. 
        :param: quarters (type list)
            A list of quarters to check for additional dealer data, string must be of the form 
            "QTRX", where X is an integer from 1-4 inclusive default = [QTR1, QTR2, QTR3, QTR4]. 
        :param: cik2brokers (type dictionary)
            A nested dictionary for storing the broker-dealer data as well as the years covered 
            from the archive e.g. {'years-reported': ['2020/QTR1', '2020/QTR2'], 'broker-dealers': 
                {1904: 'ABRAHAM SECURITIES CORPORATION'}}. 
    Return:
        :param: cik2brokers (type dict)
            Returns a dictionary with CIK:CompanyName relationships e.g. {887767: 
                1ST GLOBAL CAPITAL CORP.} as well as metadata on how many years and quarters 
                were retrieved last 
    """
    
    # archived data website for broker dealer data
    baseURL = 'https://www.sec.gov/Archives/edgar/full-index'
    
    # extract all the years covered from json form (we want to avoid re-runs) 
    archiveDates = ['{}/{}'.format(yt, qt) for yt in years for qt in quarters]
    years_covered = cik2brokers['years-covered']
    
    # itterate through years and quarters for archival search
    for coverage in archiveDates:

        if coverage in years_covered:
            print('\tWe have covered {}'.format(coverage))
            break

        else:
            searchURL = '{}/{}/form.idx'.format(baseURL, coverage)
            print(searchURL)

            # send request to SEC website to retrieve broker dealer information 
            response = requests.get(searchURL, allow_redirects=True)

            # if reponse type is active we return object with status code 200 (else error)
            if response.status_code == 200:

                # append the coverage year for the cik in question
                cik2brokers['years-covered'].append(coverage)

                # extract only main text from body, selecting terms below dashes '---' 
                # we use triple dashes to avoid improper splits that exist with company names
                data = response.text.split('---')[-1]   

                # write contents to a temporary file to read information
                with open('main.txt', 'w') as file: file.write(data)

                # convert text data to dataframe object using a fixed-width-file convention
                df = pd.read_fwf('main.txt', header=None)
                cleanDf = df[~pd.isnull(df[0])]               # strip NaNs in the Form Type column

                # use regex to check if first column contains information on X-17A-5 filings 
                x17_check = cleanDf[0].str.contains('^x-17a', regex=True, flags=re.IGNORECASE)
                x17File = cleanDf[x17_check]

                print('\tFound {} X-17A-5 filings in {}'.format(x17File.shape[0], coverage))

                # check whether X-17A-5 form type was found (if empty pass)
                if not x17File.empty:

                    # CIK number is taken from the last column of the rows splitting url string 
                    # e.g. edgar/data/886475/0001019056-10-000046.txt -> 886475
                    last_column = x17File.columns[-1]
                    cikNumbers = x17File[last_column].apply(lambda x: x.split('/')[2]).values

                    # iterate through CIK elements  
                    for elm in cikNumbers:
                        compName = companyName(elm)                       # company name for CIK 
                        cik2brokers['broker-dealers'][elm] = compName     # broker-dealer reporting

                # remove local file after it has been created
                os.remove('main.txt')
        
    return cik2brokers

# ----------------------------------------------------------
# Main Script
# ----------------------------------------------------------
    
if __name__ == "__main__":
    
    # s3 active folder for outputs
    bucket = "ran-s3-systemic-risk"
    folder ="Output/"

    # Amazon Textract client and Sagemaker session
    s3 = boto3.client('s3')
    session = Session()

    paths = session.list_s3_files(bucket, folder)

    # if the CIK-Dealer file is located in out output folder we read it in and pass as argument
    if 'Output/CIKandDealers.json' in paths: 
        # retrieving downloaded files from s3 bucket
        s3.download_file(bucket, 'Output/CIKandDealers.json', 'temp.json')
        
        # read all CIK and Dealer name information from storage
        with open('temp.json', 'r') as f: old_cik2brokers = json.loads(f.read())
            
        cik2brokers = dealerData(years=np.arange(1993, datetime.datetime.today().year+1), 
                                 cik2brokers=old_cik2brokers)   
        
        # remove local file after it has been created
        os.remove('temp.json')
    else:
        # otherwise we must run the entire history of broker-dealers registering a X-17A-5 file
        cik2brokers = dealerData(years=[1993, 1994, 1995, 1996, 1997, 1998, 1999, 2000])
        
    # write to a JSON file with accompanying meta information about coverage 
    with open('CIKandDealers.json', 'w') as file:
        json.dump(cik2brokers, file)
        file.close()
    
    # save contents to AWS S3 bucket
    with open('CIKandDealers.json', 'rb') as data:
        s3.upload_fileobj(data, bucket, 'Output/CIKandDealers.json')
        
    # remove local file after it has been pushed to the s3
    os.remove('CIKandDealers.json')
