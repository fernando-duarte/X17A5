#!/usr/bin/env python
# coding: utf-8


##################################
# INSTALL LIBRARIES
##################################

get_ipython().magic('pip install bs4')
get_ipython().magic('pip install PyPDF2')


##################################
# LIBRARY/PACKAGE IMPORTS
##################################

# interacting with Amazon AWS
import boto3
from sagemaker.session import Session

from ExtractBrokerDealers import dealerData


##################################
# MAIN CODE EXECUTION
##################################

if __name__ == "__main__":
    
    # s3 active folder for outputs
    bucket = "ran-s3-systemic-risk"
    folder ="Temp/"

    # Amazon Textract client and Sagemaker session
    s3 = boto3.client('s3')
    session = Session()

    paths = session.list_s3_files(bucket, folder)
    
    # determine the interval range for which we look back historically 
    parse_years = np.arange(1993, datetime.datetime.today().year+1)

    if 'Temp/CIKandDealers.json' in paths: 
        # retrieve old information from CIK and Dealers JSON file
        s3.download_file(bucket, 'Temp/CIKandDealers.json', 'temp.json')
        with open('temp.json', 'r') as f: old_cik2brokers = json.loads(f.read())
        
        # re-assign contents with new additional information 
        cik2brokers = dealerData(years=parse_years, cik2brokers=old_cik2brokers)   
        
        os.remove('temp.json')
    else:
        cik2brokers = dealerData(years=parse_years)
        
    # write to a JSON file with accompanying meta information about coverage 
    with open('CIKandDealers.json', 'w') as file:
        json.dump(cik2brokers, file)
        file.close()
    
    # save contents to AWS S3 bucket
    with open('CIKandDealers.json', 'rb') as data:
        s3.upload_fileobj(data, bucket, 'Temp/CIKandDealers.json')
    os.remove('CIKandDealers.json')
