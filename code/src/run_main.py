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

from GLOBAL import *
from run_pt1 import main_p1
from run_pt2 import main_p2
from run_pt3 import main_p3


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
    
    # e.g. broker_dealers_list = [782124], default handled in main_p1.py
    broker_dealers_list = [1224385, 1675365,  276523, 42352, 68136, 782124]
    
    # FLAG for determing whether we want to re-run the entire job from
    # start to finish - WITHOUT taking any existing files stored in the s3.
    # ONLY CHANGE TO 'True' if you would like to OVERWRITE pre-existing files. 
    job_rerun = False
    
##################################
# MAIN CODE EXECUTION
##################################

if __name__ == "__main__":
    
    start_time = time.time()
               
#     # responsible for gathering FOCUS reports and building list of broker-dealers
#     main_p1(
#         Parameters.bucket, Parameters.s3_pointer, Parameters.s3_session, 
#         temp_folder, input_folder_raw, input_folder_pdf_slice, 
#         input_folder_png_slice, Parameters.parse_years, Parameters.broker_dealers_list
#            )
       
#     # responsible for extracting balance-sheet figures
#     main_p2(
#         Parameters.bucket, Parameters.s3_pointer, Parameters.s3_session, 
#         temp_folder, input_folder_pdf_slice, input_folder_png_slice, 
#         output_folder_raw_pdf, output_folder_raw_png, textract, 
#         Parameters.files_to_textract, output_folder_clean_pdf, 
#         output_folder_clean_png
#            ) 
    
#     # responsible for developing structured database
#     main_p3(
#         Parameters.bucket, Parameters.s3_pointer, Parameters.s3_session, temp_folder,
#         output_folder_clean_pdf, output_folder_clean_png, output_folder_split_pdf, 
#         output_folder_split_png, output_folderasset_ml_model, liable_ml_model
#            )   
   
    elapsed_time = time.time() - start_time
    print('\n\n\nFOCUS REPORT SCRIPT COMPLETED - total time taken %.2f minutes' % elapsed_time)