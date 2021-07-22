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

# Amazon Textract client and Sagemaker session
s3_pointer = boto3.client('s3')
s3_session = Session()

# Amazon Textract client
textract = boto3.client('textract')

# folder & directory information
code_folder = 'Code/'
temp_folder ='Temp/'
input_folder = 'Input/'
output_folder = 'Output/'                

input_folder_raw = input_folder + 'X-17A-5/'
input_folder_pdf_slice = temp_folder + 'X-17A-5-PDF-SUBSETS/'
input_folder_png_slice = temp_folder + 'X-17A-5-PNG-SUBSETS/'

# files report raw Textract files
output_folder_raw_pdf = temp_folder + 'X-17A-5-PDF-RAW/'
output_folder_raw_png = temp_folder + 'X-17A-5-PNG-RAW/'

# files clean raw Textract files, handling exceptions
output_folder_clean_pdf = temp_folder + 'X-17A-5-CLEAN-PDFS/'
output_folder_clean_png = temp_folder + 'X-17A-5-CLEAN-PNGS/'

# files distinguish between assets and liability terms
output_folder_split_pdf = temp_folder + 'X-17A-5-SPLIT-PDFS/'
output_folder_split_png = temp_folder + 'X-17A-5-SPLIT-PNGS/'

# storage for machine learning models
asset_ml_model = input_folder + 'asset_log_reg_mdl_v2.joblib'
liable_ml_model = input_folder + 'liability_log_reg_mdl_v2.joblib'

# add training set
# add appended training set

