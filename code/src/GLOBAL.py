#!/usr/bin/env python
# coding: utf-8

"""
GLOBAL.py: Script storing essential global variables regarding 
mapping conventions for s3 directories
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

import boto3
from sagemaker.session import Session


##################################
# GLOBAL VARIABLES
##################################

class GlobVars:
    
    # Amazon Textract client and Sagemaker session
    s3_pointer = boto3.client('s3')
    s3_session = Session()

    # Amazon Textract client
    textract = boto3.client('textract')

    # folder & directory information
    temp_folder ='temp/'
    input_folder = 'input/'
    output_folder = 'output/'                

    input_folder_raw = input_folder + 'X-17A-5/'

    # files report reduced FOCUS reports parsed from SEC
    temp_folder_pdf_slice = temp_folder + 'X-17A-5-PDF-SUBSETS/'
    temp_folder_png_slice = temp_folder + 'X-17A-5-PNG-SUBSETS/'

    # files report raw Textract files
    temp_folder_raw_pdf = temp_folder + 'X-17A-5-PDF-RAW/'
    temp_folder_raw_png = temp_folder + 'X-17A-5-PNG-RAW/'

    # files clean raw Textract files, handling exceptions
    temp_folder_clean_pdf = temp_folder + 'X-17A-5-CLEAN-PDFS/'
    temp_folder_clean_png = temp_folder + 'X-17A-5-CLEAN-PNGS/'

    # files distinguish between assets and liability terms
    temp_folder_split_pdf = temp_folder + 'X-17A-5-SPLIT-PDFS/'
    temp_folder_split_png = temp_folder + 'X-17A-5-SPLIT-PNGS/'

    # storage for machine learning models (logistic regression)
    asset_ml_model = input_folder + 'asset_log_reg_mdl_v2.joblib'
    liable_ml_model = input_folder + 'liability_log_reg_mdl_v2.joblib'

    # storage for training-test sets for ML model
    asset_ml_ttset = input_folder + 'asset_lineitem_training_testing.csv'
    liable_ml_ttset = input_folder + 'liable_lineitem_training_testing.csv'
    
