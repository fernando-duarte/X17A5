#!/usr/bin/env python
# coding: utf-8

"""
run_file_extraction.py: Script responsible for retrieving CIKs from broker-dealers
filing FOCUS (X-17A-5) reports and downloading all relevant filings
from the SEC. We execute the following local scripts:

    1) ExtractBrokerDealers.py
    2) FocusReportExtract.py
    3) FocusReportSlicing.py
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

import os
import json
import datetime
import numpy as np
import time

from pdf2image import convert_from_path, pdfinfo_from_path
from ExtractBrokerDealers import dealerData
from FocusReportExtract import searchURL, edgarParse, fileExtract, mergePdfs
from FocusReportSlicing import selectPages, extractSubset, brokerFilter

from pdf2image.exceptions import PDFPageCountError, PDFInfoNotInstalledError

##################################
# MAIN CODE EXECUTION
##################################

def main_p1(s3_bucket, s3_pointer, s3_session, temp_folder, input_raw, export_pdf, export_png,
            parse_years, broker_dealers_list, rerun_job, company_email):
    
    # ==============================================================================
    #                 STEP 1 (Gathering updated broker-dealer list)
    # ==============================================================================
    
    print('\n========\nStep 1: Gathering Broker-Dealer Data\n========\n')
    if len(parse_years) == 0:
        parse_years = np.arange(1993, datetime.datetime.today().year+1)   
        
    s3_pointer.download_file(s3_bucket, temp_folder + 'CIKandDealers.json', 'temp.json')
    with open('temp.json', 'r') as f: old_cik2brokers = json.loads(f.read())

    # re-assign contents with new additional information 
    cik2brokers = dealerData(years=parse_years,company_email=company_email, cik2brokers=old_cik2brokers)   
    os.remove('temp.json')
   
    
    # ==============================================================================
    #                 STEP 2 (Gathering X-17A-5 Filings)
    # ==============================================================================
    
    print('\n========\nStep 2: Gathering X-17A-5 Filings\n========\n')

    broker_dealers_list = cik2brokers['broker-dealers'].keys()
          
          
    # ==============================================================================
    #                 STEP 3 (Slice X-17A-5 Filings)
    # ==============================================================================
    
    print('\n========\nStep 3: Slicing X-17A-5 Filings\n========\n')
   
    return broker_dealers_list      
