"""
PDF File Extraction for CIK (broker-dealers)

Extract URL links per company filing to download accompaning X-17A-5 files from SEC EDGAR site

@author: Rajesh Rao (Sr. Research Analyst 22')
"""

# ----------------------------------------------------------
# Package Imports
# ----------------------------------------------------------

# console and directory access
import os
import json
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

# ----------------------------------------------------------
# Function declarations
# ----------------------------------------------------------

def baseURL(cik:str, file_type:str='X-17A-5') -> str:
    """
    Constructs a base URL for searching for a paritcular SEC filing  
    
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

def edgarParse(url:str):
    """
    Parses the EDGAR webpage of a provided URL and returns a tuple of arrays/lists
    
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
    
    # requesting HTML data link from the EDGAR search results 
    response = requests.get(url, allow_redirects=True)

    # parse the HTML doc string from the response object
    soup = BeautifulSoup(response.text, 'html.parser') 
    
    # read in HTML tables from the url link provided 
    try:
        filings = pd.read_html(url)[2]                 # select the filings table from EDGAR search (IndexError Flag)
        filing_dates = filings['Filing Date'].values   # select the filing dates columns

        # parse the html-doc string for all instance of < a href= > from the URL 
        href = [link.get('href') for link in soup.find_all('a')]

        # search for all links with Archive in handle, these are the search links for the X-17A-5 filings
        archives = ['https://www.sec.gov' + link for link in href if str.find(link, 'Archives') > 0]
        
        # return a tuple of vectors, the filings dates and the corresponding urls
        return filing_dates, archives
    
    # if we can't select the filings table we flag an error
    except IndexError:
        print('Currently no filings are present for the firm\n')
        return None

def fileExtract(archive:str) -> list:
    """
    Parses through the pdf links X-17A-5 pdf files to be saved in an s3 bucket
    
    Input:
        :param: archive (type str)
            A vector of strings for all sec.gov URL links for each filings in chronological order

    Return:
        This function returns a list of pdf url links that point to the EDGAR filing for a specific
        broker-dealer at a particular year
    
    NOTE:   This script makes no effort to weed out amended releases, rather it will default to retaining 
            information on first published releases via iterative selection from the most recent filing 
    """
    
    # data is organized linearly, by most recent issue first
    # requesting data from document links storing the files
    pdf_storage = requests.get(archive, allow_redirects=True)

    # table from filing detail Edgar table 
    soup = BeautifulSoup(pdf_storage.text, 'html.parser') 

    # extracts all link within the filing table, filtering for pdfs
    extract_link = [file.get('href') for file in soup.find_all('a')]

    # filter for all pdf links from the extracted file links  
    pdf_files = [string for string in extract_link if str.find(string, 'pdf') > 0]

    return pdf_files

def mergePdfs(files:list) -> PdfFileWriter:
    """
    Combines pdfs files iteratively by page for each of the accompanying SEC filings 
    
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
        
        # request the specific pdf file from the the SEC
        pdf_storage = requests.get(pdf_file, allow_redirects=True)

        # save PDF contents to local file location 
        open('temp.pdf', 'wb').write(pdf_storage.content)
        
        # read pdf file as PyPDF2 object
        pdf = PdfFileReader('temp.pdf', strict=False) 
        nPages = pdf.getNumPages()          # detemine the number of pages in pdf
        
        # add the pages from the document as specified 
        for page_num in np.arange(nPages):
            pdfWriter.addPage(pdf.getPage(page_num))
    
    # remove temporary file on local directory
    os.remove('temp.pdf')
    
    return pdfWriter

# ----------------------------------------------------------
# Main Script
# ----------------------------------------------------------

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
    s3.download_file(bucket, 'Output/CIKandDealers.json', 'temp.json')

    # read all CIK and Dealer name information from storage
    with open('temp.json', 'r') as f: cik2brokers = json.loads(f.read())

    # remove local file after it has been created (variable is stored in memory)
    os.remove('temp.json')
    # ==============================================================================
    
    # TO BE DELETED IN FUTURE (SELECTIVE PARSES)
    big_banks = ['782124', '42352', '68136', '91154', '72267', '1224385', '851376', '853784', '58056']
    
    # iterate through a list of CIKs
    for cik_id in big_banks:
        companyName = cik2brokers['broker-dealers'][cik]      # company name for broker dealer
        
        # build lookup URLs for the SEC level data (base url)
        url = baseURL(cik)

        try:
            # return the filing dates and archived url's for each SEC company (TypeError if return None)
            filing_dates, archives = edgarParse(url)
            
             # logging info for when files are being downloaded
            print('Downloading X-17A-5 files for {} - CIK ({})'.format(companyName, cik))
            
             # itterate through each of the pdf URLs corresponding to archived contents
            for i, pdf_url in enumerate(archives):
                
                # filing date in full yyyy-MM-dd format
                date = filing_dates[i]
                
                # Construct filename & pdf file naming convention (e.g. filename = 1904-2020-02-26.pdf) 
                file_name = str(cik) + '-' + date + '.pdf'
                pdf_name = output_folder + file_name
                
                if pdf_name in s3_path: 
                    print('\tAll files for {} are downloaded'.format(companyName))
                    break
                    
                else:
                    # extract all acompanying pdf files, merging all to one large pdf
                    pdf_files = fileExtract(pdf_url)
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
                
        except TypeError:
            pass