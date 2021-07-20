#!/usr/bin/env python
# coding: utf-8


##################################
# INSTALL LIBRARIES
##################################

get_ipython().magic('pip install --upgrade pip')

# essential libraries for running Part 1
get_ipython().magic('conda update -n base -c defaults conda  # to update conda environment')
get_ipython().magic('conda install -c conda-forge poppler    # to install poppler PDF backend')

get_ipython().magic('pip install bs4')
get_ipython().magic('pip install PyPDF2')
get_ipython().magic('pip install pdf2image')
get_ipython().magic('pip install fitz')
get_ipython().magic('pip install pillow')
get_ipython().magic('pip install PyMuPDF==1.16.14')

# essential libraries for running Part 2


# essential libraries for running Part 3


##################################
# LIBRARY/PACKAGE IMPORTS
##################################
                  
import boto3
from sagemaker.session import Session

from GLOBAL import *
from RunPT_1 import main_p1
   
    
##################################
# USER DEFINED FUNCTIONS
##################################
               
class Parameters:
    
    # -------------------------------------------------
    # functional specifications file/folder locations
    # -------------------------------------------------
    
    bucket = "ran-s3-systemic-risk"
    
    # Amazon Textract client and Sagemaker session
    s3_pointer = boto3.client('s3')
    s3_session = Session()
     
    # -------------------------------------------------
    # job specific parameters specified by the user
    # -------------------------------------------------
    
    # ExtractBrokerDealers.py -> help determine the interval range for which 
    #                            we look back historically for broker dealers 
    
    # e.g. parse_years = [2019, 2020, 2021], default handled in main_p1.py
    parse_years = [2021]
        
    # FocusReportExtract.py -> extract broker-dealers from a subset or firms 
    #                          or retrieve all broker-information
    
    # e.g. broker_dealers_list = [782124], default handled in main_p1.py
    broker_dealers_list = [1224385, 1675365,  276523, 42352, 68136, 782124]
                 
        
##################################
# MAIN CODE EXECUTION
##################################

if __name__ == "__main__":
    
    main_p1(
        Parameters.bucket, Parameters.s3_pointer, Parameters.s3_session, 
        input_folder_raw, input_folder_pdf_slice, input_folder_png_slice, 
        Parameters.parse_years, Parameters.broker_dealers_list
           )
          