#!/usr/bin/env python
# coding: utf-8



##################################
# LIBRARY/PACKAGE IMPORTS
##################################

# console and directory access
import os
import re
import time 
import json
import urllib
import datetime

# interacting with Amazon AWS
import boto3
from sagemaker.session import Session

# data reading and exporting  
import pandas as pd
import numpy as np

# parsing SEC website for data  
import requests
from bs4 import BeautifulSoup

# pdf manipulation
from PyPDF2 import PdfFileReader, PdfFileWriter, utils


# ## PDF File Extraction
# Extract URL links per company filing to download accompaning X-17A-5 files from SEC EDGAR site

# In[5]:


def baseURL(cik:str, file_type:str='X-17A-5') -> str:
    """
    Constructs a base URL for searching for a paritcular SEC filing  
    ------------------------------------------------------------------------------------------
    Input:
        :param: cik (type str)
            The CIK number for a registreed broker-dealer (e.g. 1904)
        :param: file_type (type str)
            The file type looking to parse for a given broker-dealer (e.g. default X-17A-5)
            
    Return:
        :param: url (type str)
            A URL string that points to the EDGAR webpage of a registred broker dealer
            (e.g. https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=1904&type=X-17A-5&dateb=20201231)
    """
    
    # forming the SEC search URLs from the select CIK, file type and date range
    secFormat = 'https://www.sec.gov/cgi-bin/browse-edgar?'     # SEC base url
    dataSelect = 'action=getcompany&CIK={}&type={}&dateb={}'    # select params.

    # build lookup URLs for the SEC level data (base url)
    url = secFormat + dataSelect.format(cik, file_type, datetime.datetime.today().year)
    
    return url


# In[6]:


def edgarParse(url:str):
    """
    Parses the EDGAR webpage of a provided URL and returns a tuple of arrays/lists
    ------------------------------------------------------------------------------------------
    Input:
        :param: url (type str) 
            URL is a string representing a SEC website URL pointing to a CIK for X-17A-5 filings
            e.g. https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=1904&type=X-17A-5&dateb=20201231
    
    Return:
        :param: filing_dates (type numpy array)
            A vector of date strings for all X-17A-5 filings in chronological order, from newest
            to oldest in filing date (e.g. ['2020-02-26', '2019-02-28', '2018-03-02'])
        :param: archives (type list)
            A vector of strings for all sec.gov URL links for each filings in chronological order
    """
    
    response = requests.Response()
    
    # we try requesting the URL and break only if response object returns status of 200
    for _ in range(100):
        # requesting HTML data link from the EDGAR search results 
        response = requests.get(url, allow_redirects=True)
        if response.status_code == 200:
            break
    
    # last check to see if response object is "problamatic" e.g. 403
    if response.status_code != 200:
        return None
    
    # parse the HTML doc string from the response object
    soup = BeautifulSoup(response.text, 'html.parser') 
    
    # read in HTML tables from the url link provided 
    try:
        # due to web-scrapping non-constant behavior (check against 100 tries)
        for _ in range(100):
            try: 
                filings = pd.read_html(url) 
                break
            except urllib.error.HTTPError: pass
        
        filing_table = filings[2]                           # select the filings (IndexError Flag)
        filing_dates = filing_table['Filing Date'].values   # select the filing dates columns

        # parse the html-doc string for all instance of < a href= > from the URL 
        href = [link.get('href') for link in soup.find_all('a')]

        # search for all links with Archive in handle, these are the search links for the X-17A-5 filings
        archives = ['https://www.sec.gov' + link for link in href if str.find(link, 'Archives') > 0]
        
        # return a tuple of vectors, the filings dates and the corresponding urls
        return filing_dates, archives
        
    # if there exists no active reports for a given CIK, we flag the error
    except IndexError:
        print('Currently no filings are present for the firm\n')
        return None


# In[7]:


def fileExtract(archive:str) -> list:
    """
    Parses through the pdf links X-17A-5 pdf files to be saved in an s3 bucket
    ------------------------------------------------------------------------------------------
    Input:
        :param: archive (type str)
            A vector of strings for all sec.gov URL links for each filings in chronological order

    Return:
        This function returns a list of pdf url links that point to the EDGAR filing for a specific
        broker-dealer at a particular year
    
    NOTE:   This script makes no effort to weed out amended releases, rather it will default to retaining 
            information on first published releases via iterative selection from the most recent filing 
    """
    
    pdf_storage = requests.Response()
    
    # we try requesting the URL and break only if response object returns status of 200
    for _ in range(100):
        # data is organized linearly by most recent issue first, we request data from document links
        pdf_storage = requests.get(archive, allow_redirects=True)
        if pdf_storage.status_code == 200:
            break
    
    # last check to see if response object is "problamatic" e.g. 403
    if pdf_storage.status_code != 200:
        return None

    # table from filing detail Edgar table 
    soup = BeautifulSoup(pdf_storage.text, 'html.parser') 

    # extracts all link within the filing table, filtering for pdfs
    extract_link = [file.get('href') for file in soup.find_all('a')]

    # filter for all pdf links from the extracted file links  
    pdf_files = [string for string in extract_link if str.find(string, 'pdf') > 0]

    return pdf_files


# In[8]:


def mergePdfs(files:list) -> PdfFileWriter:
    """
    Combines pdfs files iteratively by page for each of the accompanying SEC filings 
    ------------------------------------------------------------------------------------------
    Input:
        :param: files (type List)
            A list of pdfs retrieved from filing details for each broker-detal in Edgar's website
            e.g. https://www.sec.gov/Archives/edgar/data/1904/000000190420000002/0000001904-20-000002-index.htm

    Return:
        :param: pdfWriter (type PdfFileWriter)
            A PdfFileWriter object that serves as a container to store each of the select pdf files from our
            list into a larger merged pdf 
    """
    
    # initialize a pdf object to be store pdf pages
    pdfWriter = PdfFileWriter()
    
    for pdf in files:
        pdf_file = 'https://www.sec.gov' + pdf 
        
        # we try requesting the URL and break only if response object returns status of 200
        for _ in range(100):
            # request the specific pdf file from the the SEC
            pdf_storage = requests.get(pdf_file, allow_redirects=True)
            if pdf_storage.status_code == 200:
                break

        # last check to see if response object is "problamatic" e.g. 403
        if pdf_storage.status_code != 200:
            continue
        
        # save PDF contents to local file location 
        open('temp.pdf', 'wb').write(pdf_storage.content)
        
        # read pdf file as PyPDF2 object
        pdf = PdfFileReader('temp.pdf', strict=False) 
        nPages = pdf.getNumPages()          # detemine the number of pages in pdf
        
        # add the pages from the document as specified 
        for page_num in np.arange(nPages):
            pdfWriter.addPage(pdf.getPage(page_num))
    
    # remove temporary file on local directory
    if os.path.isfile('./temp.pdf'):
        os.remove('temp.pdf')
    
    return pdfWriter


# In[9]:


# CIK naming conventions for broker-dealers in order of total asset, we run these in batches
# (according to https://www.auditanalytics.com/products/sec/broker-dealers)
top_9_big_banks = ['782124', '42352', '68136', '91154', '72267', '1224385', '851376', '853784', '58056']
top_18_big_banks = ['318336', '356628', '895502', '877559', '922792', '230611', '890203', '920417', '87634']
top_27_big_banks = ['26617', '1616344', '803012', '1591458', '1215680', '1146184', '867626', '1261467', '29648']


# ## Main File Execution

# In[10]:


if __name__ == "__main__":
    
    # Amazon Textract client and Sagemaker session
    s3 = boto3.client('s3')
    session = Session()
    
    bucket = 'ran-s3-systemic-risk'
    output_folder = 'Input/X-17A-5/'
    
    # ==============================================================================
    # check available pdfs stored within desired output-folder
    s3_path = session.list_s3_files(bucket, output_folder)
    
    # retrieving CIK-Dealers JSON file from s3 bucket
    s3.download_file(bucket, 'Temp/CIKandDealers.json', 'temp.json')

    # read all CIK and Dealer name information from storage
    with open('temp.json', 'r') as f: cik2brokers = json.loads(f.read())

    # remove local file after it has been created (variable is stored in memory)
    os.remove('temp.json')
    # ==============================================================================
    
    # iterate through a list of CIKs, for full list we have cik2brokers['broker-dealers'].keys()
    for cik_id in top_27_big_banks:
        companyName = cik2brokers['broker-dealers'][cik_id]
        
        # build lookup URLs for the SEC level data (base url)
        url = baseURL(cik_id)
        
        # return the filing dates and archived url's for each SEC company (TypeError if return None)
        response = edgarParse(url)
        
        if type(response) is not None:
            filing_dates, archives = response

            # logging info for when files are being downloaded
            print('Downloading X-17A-5 files for {} - CIK ({})'.format(companyName, cik_id))
            print('\t{}'.format(url))

             # itterate through each of the pdf URLs corresponding to archived contents
            for i, pdf_url in enumerate(archives):

                # filing date in full yyyy-MM-dd format
                date = filing_dates[i]

                # Construct filename & pdf file naming convention (e.g. filename = 1904-2020-02-26.pdf) 
                file_name = str(cik_id) + '-' + date + '.pdf'
                pdf_name = output_folder + file_name

                if pdf_name in s3_path: 
                    print('\tAll files for {} are downloaded'.format(companyName))
                    break

                else:
                    # extract all acompanying pdf files, merging all to one large pdf
                    pdf_files = fileExtract(pdf_url)
                    
                    if type(pdf_files) is not None:
                        concatPdf = mergePdfs(pdf_files)

                        # open file and save to local instance
                        with open(file_name, 'wb') as f:
                            concatPdf.write(f)
                            f.close()

                        # save contents to AWS S3 bucket
                        with open(file_name, 'rb') as data:
                            s3.upload_fileobj(data, bucket, pdf_name)

                        # remove local file after it has been created
                        os.remove(file_name)
                        print('\tSaved X-17A-5 files for {} on {}'.format(companyName, date))
                    
                    else:
                        print('\tNo files found for {} on {}'.format(companyName, date))
        
        # identify error in the event edgar parse (web-scrapping returns None)
        else: print('ERROR: Unable to download X-17A-5 files for {} - CIK ({})'.format(companyName, cik_id))
        


# In[ ]:




