#!/usr/bin/env python
# coding: utf-8


##################################
# INSTALL LIBRARIES
##################################

get_ipython().magic('pip install --upgrade pip')                 # upgrade pip installer

get_ipython().magic('conda update -n base -c defaults conda')    # to update conda environment
get_ipython().magic('conda install -c conda-forge poppler')      # to install poppler PDF backend
get_ipython().magic('pip install bs4')
get_ipython().magic('pip install PyPDF2')
get_ipython().magic('pip install pdf2image')
get_ipython().magic('pip install fitz')
get_ipython().magic('pip install pillow')
get_ipython().magic('pip install PyMuPDF==1.16.14')

get_ipython().magic('pip install smart_open')
get_ipython().magic('pip install minecart')
get_ipython().magic('pip install textract-trp')
get_ipython().magic('pip install python-Levenshtein')            # used for determing string similarity
get_ipython().magic('pip install fuzzywuzzy')                    # fuzzy-matching algorithms 


##################################
# LIBRARY/PACKAGE IMPORTS
##################################

from GLOBAL import *
from run_pt1 import main_p1
from run_pt2 import main_p2
from run_pt3 import main_p3
   
    
##################################
# USER DEFINED FUNCTIONS
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
        
        
    # FocusReportExtract.py -> extract broker-dealers from a subset or firms 
    #                          or retrieve all broker-information, default is 
    #                          an empty list
    
    # e.g. broker_dealers_list = [782124], default handled in main_p1.py
    broker_dealers_list = [1224385, 1675365,  276523, 42352, 68136, 782124]
        
        
    # OCRTextract.py ->  determine which files should be passed through Textract
    #                    to extract balance-sheets, default is an empty list
    
    # e.g. files_to_textract = ['Input/X-17A-5-PDF-SUBSETS/72267-2014-05-30-subset.pdf']
    files_to_textract = ['Input/X-17A-5-PDF-SUBSETS/72267-2014-05-30-subset.pdf']
    
    
##################################
# MAIN CODE EXECUTION
##################################

if __name__ == "__main__":
    
    # responsible for gathering FOCUS reports and building list of broker-dealers
    main_p1(
        Parameters.bucket, Parameters.s3_pointer, Parameters.s3_session, 
        input_folder_raw, input_folder_pdf_slice, input_folder_png_slice, 
        Parameters.parse_years, Parameters.broker_dealers_list
           )
       
    # responsible for extracting balance-sheet figures
    main_p2(
        Parameters.bucket, Parameters.s3_pointer, Parameters.s3_session, 
        input_folder_pdf_slice, input_folder_png_slice, 
        output_folder_raw_pdf, output_folder_raw_png,
        textract, files_to_textract, output_folder_clean_pdf, 
        output_folder_clean_png
           ) 
    
    # responsible for developing structured database
    main_p3(
        Parameters.bucket, Parameters.s3_pointer, Parameters.s3_session
           )     