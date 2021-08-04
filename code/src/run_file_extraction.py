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

from pdf2image import convert_from_path
from ExtractBrokerDealers import dealerData
from FocusReportExtract import searchURL, edgarParse, fileExtract, mergePdfs
from FocusReportSlicing import selectPages, extractSubset, brokerFilter

from pdf2image.exceptions import PDFPageCountError, PDFInfoNotInstalledError

##################################
# MAIN CODE EXECUTION
##################################

def main_p1(s3_bucket, s3_pointer, s3_session, temp_folder, input_raw, export_pdf, export_png,
            parse_years, broker_dealers_list, rerun_job):
    
    # ==============================================================================
    #                 STEP 1 (Gathering updated broker-dealer list)
    # ==============================================================================
    
    print('\n========\nStep 1: Gathering Broker-Dealer Data\n========\n')
    
    # all s3 files corresponding within folders 
    temp_paths = s3_session.list_s3_files(s3_bucket, temp_folder)
    
    # if no years are provided by the user, we default to the full sample
    if len(parse_years) == 0:
        parse_years = np.arange(1993, datetime.datetime.today().year+1)
    
    # if rerun_job is True, we overwrite our current CIKandDealer information on s3
    if (temp_folder + 'CIKandDealers.json' in temp_paths) and (rerun_job == False): 
        
        # retrieve old information from CIK and Dealers JSON file
        s3_pointer.download_file(s3_bucket, temp_folder + 'CIKandDealers.json', 'temp.json')
        with open('temp.json', 'r') as f: old_cik2brokers = json.loads(f.read())
        
        # re-assign contents with new additional information 
        cik2brokers = dealerData(years=parse_years, cik2brokers=old_cik2brokers)   
        os.remove('temp.json')
        
    else:
        # start from scratch to create cik-broker dealer list
        cik2brokers = dealerData(years=parse_years)
        
    # write to a local JSON file with accompanying meta information 
    with open('CIKandDealers.json', 'w') as file:
        json.dump(cik2brokers, file)
        file.close()
    
    # save contents to AWS S3 bucket
    with open('CIKandDealers.json', 'rb') as data:
        s3_pointer.upload_fileobj(data, s3_bucket, temp_folder + 'CIKandDealers.json')
    os.remove('CIKandDealers.json')
    
    # ==============================================================================
    #                 STEP 2 (Gathering X-17A-5 Filings)
    # ==============================================================================
    
    print('\n========\nStep 2: Gathering X-17A-5 Filings\n========\n')
    
    input_paths = s3_session.list_s3_files(s3_bucket, input_raw)
          
    # if no broker-dealers are provided by the user, we default to the full sample
    if len(broker_dealers_list) == 0:
        broker_dealers_list = cik2brokers['broker-dealers'].keys()

    for cik_id in broker_dealers_list:
        companyName = cik2brokers['broker-dealers'][cik_id]
        
        # build lookup URLs to retrieve filing dates and archived url's
        url = searchURL(cik_id)
        response = edgarParse(url)
        
        if type(response) is not None:
            filing_dates, archives = response

            # logging info for when files are being downloaded
            print('Downloading X-17A-5 files for %s - CIK (%s)' % (companyName, cik_id))
            print('\t%s' % url)

            # itterate through each of the pdf URLs corresponding to archived contents
            for i, pdf_url in enumerate(archives):

                # filing date in full yyyy-MM-dd format
                date = filing_dates[i]      
          
                # Construct filename & pdf file naming convention (e.g. filename = 1904-2020-02-26.pdf)   
                file_name = str(cik_id) + '-' + date + '.pdf'        
                pdf_name = input_raw + file_name
                
                # if rerun_job is true, we ignore the flag and extract focus reports
                # for every reported year in the reponse object for filings
                if (pdf_name in input_paths) and (rerun_job == False): 
                    print('\tAll files for %s are downloaded' % companyName)
                    break

                else:
                    # extract all acompanying pdf files, merging all to one large pdf
                    pdf_files = fileExtract(pdf_url)
                    
                    # make sure we don't return empty lists
                    if len(pdf_files) > 0:
                        print('\tExtracting FOCUS filing for %s' % date)
                        concatPdf = mergePdfs(pdf_files)

                        # open file and save to local instance
                        with open(file_name, 'wb') as f:
                            concatPdf.write(f)
                            f.close()

                        # save contents to AWS S3 bucket
                        with open(file_name, 'rb') as data:
                            s3_pointer.upload_fileobj(data, s3_bucket, pdf_name)
                        os.remove(file_name)
                    
                    else: print('\tNo files found for %s on %s' % (companyName, date))
        
        # identify error in the event edgar parse (web-scrapping returns None)
        else: print('WEB-SCRAPPING ERROR: Unable to download %s - CIK (%s), no filing' % (companyName, cik_id))
          
    # ==============================================================================
    #                 STEP 3 (Slice X-17A-5 Filings)
    # ==============================================================================
    
    print('\n========\nStep 3: Slicing X-17A-5 Filings\n========\n')
    
    # re-run input paths post file extraction to update directory
    input_paths = s3_session.list_s3_files(s3_bucket, input_raw)
    
    pdf_paths = s3_session.list_s3_files(s3_bucket, export_pdf)
    png_paths = s3_session.list_s3_files(s3_bucket, export_png)
    
    # filter FOCUS reports from the s3 that correspond to list of broker-dealers 
    raw_broker_dealer_pdfs = filter(lambda x: brokerFilter(broker_dealers_list, x), input_paths)
     
    for path_name in raw_broker_dealer_pdfs:
        print('Slicing FOCUS report filing for %s' % path_name)
        
        # check to see if values are downloaded to s3 sub-bin
        base_file = path_name.split('/')[-1].split('.')[0]
        pdf_look_up = export_pdf + base_file + '-subset.pdf'
        png_look_up = export_png + base_file + '/' + base_file + '-p0.png'
        
        # ---------------------------------------------------------------
        # PDF FILE DOWNLOAD
        # ---------------------------------------------------------------
        
        if (pdf_look_up in pdf_paths) and (rerun_job == False):
            print('\t%s already saved pdf' % base_file)

        else: 
            # retrieving downloaded files from s3 bucket
            s3_pointer.download_file(s3_bucket, path_name, 'temp.pdf')
            
            # run the subset function to save a local subset file (void-function)
            export_name = base_file + '-subset.pdf'
            extractSubset(range(20), export_name)        # first twenty pages
            
             # save contents to AWS S3 bucket as specified
            with open(export_name, 'rb') as data:
                s3_pointer.upload_fileobj(data, s3_bucket, export_pdf + export_name)
                print('\tSaved pdf files for -> %s' % export_name)
            
            # remove local file after it has been created
            os.remove('temp.pdf')
            os.remove(export_name)
        
        # ---------------------------------------------------------------
        # PNG FILE DOWNLOAD
        # ---------------------------------------------------------------
        
        if (png_look_up in png_paths) and (rerun_job == False):
            print('\t%s already saved png' % base_file)
            
        else: 
            # retrieving downloaded files from s3 bucket
            s3_pointer.download_file(s3_bucket, path_name, 'temp.pdf')
            
            try:
                # document class for temporary pdf (correspond to X-17A-5 pages)  
                pages = convert_from_path('temp.pdf', 500)
                
                # determine the iterable size (number of page in document)
                if len(pages) > 20:
                    size = 20
                else: size = len(pages)
                
                for idx in range(size):
                    # write the png name for exportation
                    export_file_name = "{}-p{}.png".format(base_file, idx)
                    
                    # storing PDF page as a PNG file locally (using pdf2image)
                    pages[idx].save(export_file_name, 'PNG')
                    
                    # save contents to AWS S3 bucket as specified
                    with open(export_file_name, 'rb') as data:
                        s3_pointer.upload_fileobj(data, s3_bucket, export_png + base_file + '/' + export_file_name)
                    
                    os.remove(export_file_name)
                    
                print('\tSaved png files for -> %s' % base_file)
                
            except PDFPageCountError:
                print('\tEncountered PDFPageCounterError when trying to convert to PNG for -> %s' % base_file)
                
            except PDFInfoNotInstalledError:
                print('\tUnable to get page count, may be a bug in the libpoppler library for PNG.')
            
            # remove local file after it has been created
            os.remove('temp.pdf')
            
    return broker_dealers_list      
