#!/usr/bin/env python
# coding: utf-8

"""
FocusReportExtract.py: Responsbile for extracting FOCUS reports (X-17A-5)
from registered broker-dealers in the SEC's history
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

# console and directory access
import os
import re
import time
import urllib
import datetime

# structured data reading 
import pandas as pd
import numpy as np

# parsing SEC website for data  
import requests
from bs4 import BeautifulSoup

# pdf manipulation
from PyPDF2 import PdfFileReader, PdfFileWriter, utils

# module that deals with encryption error
from pikepdf import Pdf


##################################
# USER DEFINED FUNCTIONS
##################################

def searchURL(cik:str, file_type:str='X-17A-5') -> str:
    """
    Constructs a base URL for searching for a SEC filing 
    
    Parameters
    ----------
    cik : str
        The CIK number for a registered broker-dealer (e.g. 1904)
        
    file_type : str
        The file type looking to parse for a given 
        broker-dealer (e.g. default X-17A-5)
    """
    
    # forming the SEC search URLs from CIK and file type
    secFormat = 'https://www.sec.gov/cgi-bin/browse-edgar?'         # SEC base url
    dataSelect = 'action=getcompany&CIK={}&type={}&dateb={}1231'    # select params.

    # build lookup URLs for the SEC level data (base url - with most current year)
    # (e.g. https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=1904&type=X-17A-5&dateb=20201231)
    url = secFormat + dataSelect.format(cik, file_type, datetime.datetime.today().year)
    
    return url

def edgarParse(url:str, company_email:str):
    """
    Parses the EDGAR webpage of a provided URL and returns 
    a tuple of filings dates and archived filings URLs
    
    Parameters
    ----------
    url : str 
        URL is a string representing a SEC website URL 
        pointing to a CIK for X-17A-5 filings
    company_email : str
        The company email belonging to the user e.g. mathias.andler@ny.frb.org
    """
    
    response = requests.Response()
    
    # we try requesting the URL and break only if response object returns status of 200
    for _ in range(20): 
        response = requests.get(url, headers={'User-Agent': 'Company Name ' + company_email},
                           stream=True, allow_redirects=True)
        
        time.sleep(1)
        if response.status_code == 200: break
    
    # last check to see if response object is "problematic" e.g. 403 after 10 tries
    if response.status_code != 200: return None
    
    # parse the HTML doc string from the response object
    soup = BeautifulSoup(response.text, 'html.parser') 
    
    # read in HTML tables from the url link provided 
    try:
        # due to web-scrapping non-constant behavior (check against few tries)
        for _ in range(20):
            try: 
                #filings = pd.read_html(url) 
                r = requests.get(url, headers={'User-Agent': 'Company Name ' + company_email})
                filings = pd.read_html(r.text)

                break
            except urllib.error.HTTPError: print('HTTPError: Unable to read URL %s' % url)
        
        # case handle in the event we return nothing
        try:
            filing_table = filings[2]                           # select the filings (raises IndexError Flag)
            filing_dates = filing_table['Filing Date'].values   # select the filing dates columns

            # parse the html-doc string for all instance of < a href= > from the URL 
            href = [link.get('href') for link in soup.find_all('a')]

            # search for all links with Archive in handle, these are the search links for the X-17A-5 filings
            archives = ['https://www.sec.gov' + link for link in href if str.find(link, 'Archives') > 0]

            # return a tuple of vectors, the filings dates and the corresponding urls
            return filing_dates, archives
        except UnboundedLocalError:
            return None
        
    # if there exists no active reports for a given CIK, we flag the error
    except IndexError:
        print('Currently no filings are present for the firm\n')
        return None

def fileExtract(archive:str, company_email:str) -> list:
    """
    Parses through the X-17A-5 filings links to  
    to be saved in an s3 bucket on AWS
    
    Parameters
    ----------
    archive : str
        A vector of strings for all sec.gov URL links 
        for each filings in chronological order
        
    company_email : str
        The company email belonging to the user e.g. mathias.andler@ny.frb.org
    """
    
    pdf_storage = requests.Response()
    
    # we try requesting the URL and break only if response object returns status of 200
    for _ in range(20):
        pdf_storage = requests.get(archive, headers={'User-Agent': 'Company Name ' + company_email},
                           stream=True, allow_redirects=True)
        time.sleep(1)
        if pdf_storage.status_code == 200: break
        
    # last check to see if response object is "problamatic" e.g. 403
    if pdf_storage.status_code != 200: 
        return []

    # table from filing detail Edgar table 
    soup = BeautifulSoup(pdf_storage.text, 'html.parser') 

    # extracts all link within the filing table, filtering for pdfs
    extract_link = [file.get('href') for file in soup.find_all('a')]

    # filter for all pdf links from the extracted file links  
    pdf_files = [string for string in extract_link if str.find(string, 'pdf') > 0]

    return pdf_files

def mergePdfs(files:list, company_email:str,second_pass=False) -> PdfFileWriter:
    """
    Combines pdfs files iteratively by page for 
    each of the accompanying SEC filings 
    
    Parameters
    ----------
    files : list
        A list of pdfs retrieved from filing details 
        for each broker-detal in Edgar's website
    company_email : str
        The company email belonging to the user e.g. mathias.andler@ny.frb.org
    second_pass: Boolean
        Some recent (2020) pdfs make the code break right after mergePdf is called (I believe it's some encryption issue)
        If second_pass == True, we pass the pdf through pikepdf to solve this issue (in the except catch)
    """
    
    # initialize a pdf object to be store pdf pages
    pdfWriter = PdfFileWriter()
    
    for pdf in files:
        pdf_file = 'https://www.sec.gov' + pdf 
        
        # we try requesting the URL and break only if response object returns status of 200
        for _ in range(20):
            pdf_storage = requests.get(pdf_file, headers={'User-Agent': 'Company Name ' + company_email},
                           stream=True, allow_redirects=True)
            time.sleep(1)
            if pdf_storage.status_code == 200: break

        # last check to see if response object is "problematic" e.g. 403
        if pdf_storage.status_code != 200: 
            continue
            
        # save PDF contents to local file location 
        open('temp.pdf', 'wb').write(pdf_storage.content)
        # read pdf file as PyPDF2 object
        try:
            pdf = PdfFileReader('temp.pdf', strict=False) 
            nPages = pdf.getNumPages()
            if second_pass:
                raise
        except:
            with Pdf.open('temp.pdf', allow_overwriting_input=True) as pdf:
                pdf.save('temp.pdf')
            
            pdf = PdfFileReader('temp.pdf', strict=False) 
            nPages = pdf.getNumPages()
        
        # add the pages from the document as specified 
        for page_num in np.arange(nPages):
            pdfWriter.addPage(pdf.getPage(page_num))
    
    if os.path.isfile('./temp.pdf'): os.remove('temp.pdf')
    
    return pdfWriter

