#!/usr/bin/env python
# coding: utf-8

"""
GLOBAL.py: Script storing essential global variables regarding 
mapping conventions for s3 directories
"""

temp_folder ="Temp/"
input_folder = 'Input/'
output_folder = 'Output/'                

input_folder_raw = input_folder + 'X-17A-5/'
input_folder_pdf_slice = input_folder + 'X-17A-5-PDF-SUBSETS/'
input_folder_png_slice = input_folder + 'X-17A-5-PNG-SUBSETS/'

# files report raw Textract files
output_folder_raw_pdf = output_folder + 'X-17A-5-PDF-RAW/'
output_folder_raw_pdf = output_folder + 'X-17A-5-PDF-RAW/'

# files clean raw Textract files, handling exceptions
output_folder_clean_pdf = output_folder + 'X-17A-5-CLEAN-PDFS/'
output_folder_clean_pdf = output_folder + 'X-17A-5-CLEAN-PNGS/'

# files distinguish between assets and liability terms
output_folder_raw_pdf = output_folder + 'X-17A-5-SPLIT-PDFS/'
output_folder_raw_pdf = output_folder + 'X-17A-5-SPLIT-PNGS/'
        