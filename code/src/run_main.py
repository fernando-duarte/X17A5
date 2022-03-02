#!/usr/bin/env python
# coding: utf-8

"""
Project is run on Python 3.6x

PLEASE READ THE DOCUMENTATION FROM pdf2image provided at the GitHub
link (https://github.com/Belval/pdf2image). You will need to install
poppler on your machine to run this code. 
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

import sys
import time
import numpy as np
from GLOBAL import GlobVars
# from run_file_extraction import main_p1
from run_file_extraction_fast import main_p1
#from run_ocr import main_p2
#from run_ocr_last_thousand import main_p2
from run_ocr_blocks import main_p2

#from run_build_database import main_p3
#from run_build_database_paral import main_p3
from run_build_database_blocks import main_p3


##################################
# USER DEFINED PARAMETERS
##################################
               
class Parameters:
    
    # -------------------------------------------------
    # functional specifications file/folder locations
    # -------------------------------------------------
    
    bucket = "x17-a5-mathias-version-nit"
    
    # -------------------------------------------------
    # job specific parameters specified by the user
    # -------------------------------------------------
    
    # Due to changes in the SEC url pulls users must provide additional 
    # webscrapping credentials to verify non-malicious actions to server (11/2021) 
    
    # e.g. requests.get(sec_url, headers={'User-Agent': 'Company Name mathias.andler@ny.frb.org'},
    #                   stream=True, allow_redirects=True))
    company_email = 'mathias.andler@ny.frb.org'
    
    # ExtractBrokerDealers.py -> help determine the interval range for which 
    #                            we look back historically for broker dealers, 
    #                            default is an empty list 
    
    # e.g. parse_years = [2019, 2020, 2021], default (empty list) handled in run_file_extraction.py 
    parse_years = np.arange(1993,2022)
        
    # FocusReportExtract.py -> extract broker-dealers from a subset of firms 
    #                          or retrieve all broker-information, default is 
    #                          an empty list
    
    # e.g. broker_dealers_list = ['782124'], default (empty list = [] ) handled in run_file_extraction.py
<<<<<<< HEAD
    #broker_dealers_list = ['782124', '42352', '68136', '91154', '72267']
    broker_dealers_list = []
=======
    # broker_dealers_list = ['782124', '42352', '68136', '91154', '72267']
    broker_dealers_list = []
	
    # broker_dealers_list =  ['318336','230611','87634','853784','851376','782124','42352','68136','230611','895502','48966','860220','808379','58056','754542','200565','753835','65106','874362','91154','703004','1101180','318336','276523','1675365']
>>>>>>> dfd8abe91f5895aa11bf736a0ae0940fd6ff55de

    # FLAG for determing whether we want to re-run parts (or the entire) job
    # - WITHOUT taking existing files stored in the s3.
    # ONLY CHANGE TO a number different from 9 if you would like to OVERWRITE pre-existing files. 

    # for example: job_rerun = 1 : we are starting the whole job from step 1 (assume no files pre-exist)
    #              job_rerun = 2 : assume step 1 was completed and start from step 2, which means we are downloading all X17-A files for       #                              all dates again
    #              job_rerun = 3 : important option. If step 2 broke, then you should run with this option, as it will not download files       #                              that were just downloaded before the crash
    #                             
    #              job_rerun = 9 : assume all files were already downloaded and processed
                   
    
    job_rerun = 9
    
    
##################################
# MAIN CODE EXECUTION
##################################

if __name__ == "__main__":
    
    import os
    os.environ['http_proxy'] = "http://p1proxy.frb.org:8080"
    os.environ['https_proxy'] = "http://p1proxy.frb.org:8080"    
    start_time = time.time()    
    print('\n\nFor details on repository refer to GitHub repo at https://github.com/raj-rao-rr/X17A5\n')
    import os
    os.environ['http_proxy'] = "http://p1proxy.frb.org:8080"
    os.environ['https_proxy'] = "http://p1proxy.frb.org:8080" 
    # responsible for gathering FOCUS reports and building list of broker-dealers
    bk_list = main_p1(
        Parameters.bucket, GlobVars.s3_pointer, GlobVars.s3_session, 
        GlobVars.temp_folder, GlobVars.input_folder_raw, GlobVars.temp_folder_pdf_slice, 
        GlobVars.temp_folder_png_slice, Parameters.parse_years, Parameters.broker_dealers_list,
        Parameters.job_rerun, Parameters.company_email
           )
     
    # responsible for extracting balance-sheet figures by OCR via AWS Textract
    """
    main_p2(
        Parameters.bucket, GlobVars.s3_pointer, GlobVars.s3_session, 
        GlobVars.temp_folder, GlobVars.temp_folder_pdf_slice, GlobVars.temp_folder_png_slice, 
        GlobVars.temp_folder_raw_pdf, GlobVars.temp_folder_raw_png, GlobVars.textract, 
        GlobVars.temp_folder_clean_pdf, GlobVars.temp_folder_clean_png, Parameters.job_rerun,
        bk_list
           ) 
    
    # responsible for developing structured and unstructured database
    """
    main_p3(
        Parameters.bucket, GlobVars.s3_pointer, GlobVars.s3_session, GlobVars.temp_folder,
        GlobVars.temp_folder_clean_pdf, GlobVars.temp_folder_clean_png, GlobVars.temp_folder_split_pdf, 
        GlobVars.temp_folder_split_png, GlobVars.output_folder, GlobVars.asset_ml_model, 
        GlobVars.liable_ml_model, GlobVars.asset_ml_ttset, GlobVars.liable_ml_ttset,
        Parameters.job_rerun, bk_list
           )   
    
