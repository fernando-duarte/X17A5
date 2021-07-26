#!/usr/bin/env python
# coding: utf-8

"""
Project is run on Python 3.7x

PLEASE READ THE DOCUMENTATION FROM pdf2image provided at the GitHub
link (https://github.com/Belval/pdf2image). You will need to install
poppler on your machine to run this code. 
"""

##################################
# LIBRARY/PACKAGE IMPORTS (pip)
##################################

import sys
import time
import subprocess


##################################
# INSTALL LIBRARIES (subprocess)
##################################

subprocess.check_call([sys.executable, '-m', 'pip', 'install', 
'--upgrade', 'pip'])

subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'bs4'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'PyPDF2'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pdf2image'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'fitz'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pillow'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'PyMuPDF==1.16.14'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'smart_open'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'minecart'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'textract-trp'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'python-Levenshtein'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'fuzzywuzzy'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'joblib'])
subprocess.check_call([sys.executable, '-m', 'pip', 'install', '-U', 'scikit-learn'])


##################################
# LIBRARY/PACKAGE IMPORTS (code)
##################################

from GLOBAL import GlobVars
from run_file_extraction import main_p1
from run_ocr import main_p2
from run_build_database import main_p3


##################################
# USER DEFINED PARAMETERS
##################################
               
class Parameters:
    
    # -------------------------------------------------
    # functional specifications file/folder locations
    # -------------------------------------------------
    
    bucket = "ran-s3-systemic-risk"
    
    # -------------------------------------------------
    # job specific parameters specified by the user
    # -------------------------------------------------
    
    # ExtractBrokerDealers.py -> help determine the interval range for which 
    #                            we look back historically for broker dealers, 
    #                            default is an empty list 
    
    # e.g. parse_years = [2019, 2020, 2021], default handled in main_p1.py
    parse_years = [2021]
        
        
    # FocusReportExtract.py -> extract broker-dealers from a subset of firms 
    #                          or retrieve all broker-information, default is 
    #                          an empty list
    
    # e.g. broker_dealers_list = ['782124'], default handled in main_p1.py
    broker_dealers_list = ['87634']
    
    
    # FLAG for determing whether we want to re-run the entire job from
    # start to finish - WITHOUT taking any existing files stored in the s3.
    # ONLY CHANGE TO 'True' if you would like to OVERWRITE pre-existing files. 
    job_rerun = False
    
##################################
# MAIN CODE EXECUTION
##################################

if __name__ == "__main__":
    
    start_time = time.time()    
    print('\n\n\n')
    
    # responsible for gathering FOCUS reports and building list of broker-dealers
    bk_list = main_p1(
        Parameters.bucket, GlobVars.s3_pointer, GlobVars.s3_session, 
        GlobVars.temp_folder, GlobVars.input_folder_raw, GlobVars.temp_folder_pdf_slice, 
        GlobVars.temp_folder_png_slice, Parameters.parse_years, Parameters.broker_dealers_list,
        Parameters.job_rerun
           )
     
    # responsible for extracting balance-sheet figures by OCR via AWS Textract
    main_p2(
        Parameters.bucket, GlobVars.s3_pointer, GlobVars.s3_session, 
        GlobVars.temp_folder, GlobVars.temp_folder_pdf_slice, GlobVars.temp_folder_png_slice, 
        GlobVars.temp_folder_raw_pdf, GlobVars.temp_folder_raw_png, GlobVars.textract, 
        GlobVars.temp_folder_clean_pdf, GlobVars.temp_folder_clean_png, Parameters.job_rerun,
        bk_list
           ) 
    
    # responsible for developing structured and unstructured database
    main_p3(
        Parameters.bucket, GlobVars.s3_pointer, GlobVars.s3_session, GlobVars.temp_folder,
        GlobVars.temp_folder_clean_pdf, GlobVars.temp_folder_clean_png, GlobVars.temp_folder_split_pdf, 
        GlobVars.temp_folder_split_png, GlobVars.output_folder, GlobVars.asset_ml_model, 
        GlobVars.liable_ml_model, GlobVars.asset_ml_ttset, GlobVars.liable_ml_ttset,
        Parameters.job_rerun, bk_list
           )   
   
    elapsed_time = time.time() - start_time
    print('\n\nFOCUS REPORT SCRIPT COMPLETED - total time taken %.2f minutes' % (elapsed_time / 60))
    