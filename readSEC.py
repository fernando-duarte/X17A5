#!/apps/Anaconda3-2019.03/bin/python 
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  9 11:54:58 2020

Pulls X-17A-5 pdf files from the SEC website for all listed broker dealers.

@author: Rajesh Rao
"""

##########################################################################
# PACKAGE IMPORTS
##########################################################################

# console and directory access
import os
import shutil

# interacting with Amazon AWS
import boto3
from sagemaker.session import Session

# data reading and exporting  
import pandas as pd
import json
import tempfile

# parsing SEC website for data  
import requests
import time 
from bs4 import BeautifulSoup


##########################################################################
# Function Handles 
##########################################################################
def secParse(completeYear:str, nLinks:int, secURLS:list, cik2brokers:dict, bdNames:list, file_type:str, 
             bucket:str, subFolder:str=None):
    """
    Parses SEC website for X-17A-5 files (void function -> no return)
    :param nLinks: (int) the number of links to extract from
    """
    # initialize time for process to run 
    startTime = time.time()
    
    # Amazon Textract client and Sagemaker session
    s3 = boto3.client('s3')
    session = Session()
    
    # discover all of the pdfs that you want to parse
    s3_path = session.list_s3_files(bucket, subFolder)
    
    # the URL links for each SEC company
    for url_link in range(nLinks):

        # company name for broker dealer being downloaded
        companyName = cik2brokers[bdNames[url_link]]

        # logging info for when files are being downloaded
        print('{} - Downloading {} files for {}'.format(url_link, file_type, companyName))

        # requesting HTML data link from the EDGAR search results 
        response = requests.get(secURLS[url_link], allow_redirects=True)

        # parse the HTML doc string from the response object
        s1Table = BeautifulSoup(response.text, 'html.parser') 

        # parse the html-doc string for all instance of <a href=>
        for link in s1Table.find_all('a'):
            documentURL = link.get('href')           # document links for filings

            try:
                # Check for Archives header as those are contained in the filings
                check = documentURL.split('/')[1]    
                if check == 'Archives':                 

                    # document URL link for each SEC filing for given year 
                    pdf_url = 'https://www.sec.gov' + documentURL
                    year = documentURL.split('-')[1] 
                    
                    # if years match this implies a file has been amended 
                    # data is organized linearly, by most recent issue first
                    if completeYear != year:

                        # requesting data from document links storing the files
                        pdf_storage = requests.get(pdf_url, allow_redirects=True)

                        # table from filing detail Edgar table 
                        s2Table = BeautifulSoup(pdf_storage.text, 'html.parser') 

                        # extracts all link within the filing table, filtering for pdfs
                        filing_detail = s2Table.find_all('a')
                        extract_link = [file.get('href') for file in filing_detail]

                        # filter for all pdf links from the extracted file links  
                        pdf_files = [string for string in extract_link if 'pdf' in string]
                        pdf_file = 'https://www.sec.gov' + pdf_files[-1] 

                        # storing pdf details within the folder
                        pdf_name = 'X17A5/' + cik2brokers[bdNames[url_link]] + year + '.pdf'

                        # if pdf file is stored locally avoid running script further
                        # simply continue to other company (we assume all present)
                        if pdf_name in s3_path: 
                            print('\tAll files for {} are downloaded'.format(companyName))
                            break

                        else:
                            # request the specific pdf file from the the SEC
                            pdf_storage = requests.get(pdf_file, allow_redirects=True)
                            
                            # filename for the pdf to be stored in s3
                            fileName = cik2brokers[bdNames[url_link]] + year + '.pdf'
                            
                            # save PDF contents to local file location 
                            open(fileName, 'wb').write(pdf_storage.content)
                            
                            # save contents to AWS S3 bucket
                            with open(fileName, 'rb') as data:
                                s3.upload_fileobj(data, bucket, subFolder+fileName)
                                
                            # remove local file after it has been created
                            os.remove(fileName)
                            
                            print('\tSaved {} files for {} year {}'.format(file_type, companyName, year))
                            completeYear = year 

            # if documentURL has no split greater than length of 1, false link       
            except IndexError:
                pass

    print('Time taken in seconds is {}'.format(time.time() - startTime))
    
# %%
##########################################################################
# DEALER DATA IMPORT
# Parses in dealer information with accompaning CIK code for EDGAR lookup
##########################################################################

if os.path.isfile(basePath+'secRegisteredDealers.txt'):
    
    # exporting RegisteredDealer information, loading in Json dictionary 
    with open(basePath+'secRegisteredDealers.txt') as file:
        cik2brokers = json.load(file)
    
    # unpacking the dictionary values (all broker dealer company names)
    bdNames = [*cik2brokers]
    
else:
    print('File not found, retrieving information')
    
    # reading in the broker dealer information
    brokerDealers = pd.read_excel(basePath+'broker-dealer-data_FY2018_Sample.xlsx')

    # replace weird name formats from broker-dealer info
    params = {
            'A.G.P. / ALLIANCE GLOBAL PARTNERS CORP.':'A.G.P.',
            'ACA/PRUDENT INVESTORS PLANNING CORPORATION':'ACA',
            'BROWN, LISLE/CUMMINGS, INC.':'CUMMINGS, INC.',
            'COLLINS/BAY ISLAND SECURITIES, LLC':'COLLINS',
            'FRANKLIN/TEMPLETON DISTRIBUTORS, INC.':'FRANKLIN',
            'HCFP/CAPITAL MARKETS LLC':'CAPITAL MARKETS LLC',
            'TEMPLETON/FRANKLIN INVESTMENT SERVICES,INC.':'TEMPLETON',
            }
    brokerDealers['Company'] = brokerDealers[['Company']].replace(params)
    
    # broker dealers assigned by unique CIK Codes, casting to integer type
    bdNames = brokerDealers['CIK Code'].unique()[:-1]
    bdNames = bdNames.astype(int)
    
    # used to reference the company name from the CIK code
    cik2brokers = brokerDealers[['CIK Code', 'Company']].set_index('CIK Code')
    cik2brokers = cik2brokers.iloc[:-3].to_dict()['Company']
    
    # exporting RegisteredDealer information
    with open(basePath+'secRegisteredDealers.txt', 'w') as file:
        json.dump(cik2brokers, file)
        file.close()
     
# %%
##########################################################################
# RUN SCRIPT (main file)
##########################################################################
if __name__ == '__main__':
    
    ############ MANUAL VARIABLES #############

    file_type = 'X-17A-5'       # files looking to extract
    prior2date = '20201231'     # format YYYY/MM/DD - select data prior to this data

    ###########################################
    
    basePath = os.getcwd()+'/Input/'

    n = len(bdNames)                # number of broker/dealers
    secURLS = [0]*n                 # initialize memory for url container  
    startTime = time.time()         # set tuner to track code 
    completeYear = None             # initialize year to check for updates

    secFormat = 'https://www.sec.gov/cgi-bin/browse-edgar?'     # SEC base url
    dataSelect = 'action=getcompany&CIK={}&type={}&dateb={}'    # select params.

    # iterate through the CIK's from the broker dealers to build lookup URLs -> o(n) runtime
    for name in range(n):
        companyCIK = bdNames[name][:-2]         # e.g. 782124.0 -> 782124
        url = secFormat + dataSelect.format(companyCIK, file_type, prior2date)
        secURLS[name] = url                     # storing the search URLs
    
    # AWS s3 bucket and subfolder 
    bucket = "ran-s3-systemic-risk"
    data_folder ="X17A5/"
    
    # call function to parse data from the SEC -> port to s3
    secParse(prior2date[:4], n, secURLS, cik2brokers, bdNames, file_type, bucket, data_folder)