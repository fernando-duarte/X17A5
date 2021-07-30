#!/usr/bin/env python
# coding: utf-8

"""
run_ocr.py: Script responsible for performing OCR via AWS Textract, 
and then "cleaning" the reported dataframes by handling special 
Textract errors and converting the read strings as numeric values

    1) OCRTextract.py
    2) OCRClean.py
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

import os
import json
import numpy as np

from OCRTextract import runJob, readTable, readForm, readText  


##################################
# MAIN CODE EXECUTION
##################################

def practice_main(s3_bucket:str, single_file:str):
    
    print('\nCurrently working on Textract extraction for %s\n================================\n' % single_file)
    
    # temporary data frame object for balance sheet information
    res = runJob(s3_bucket, single_file)
    
    # if Textract job did not fail we continue extraction
    if res[0]['JobStatus'] != 'FAILED':
        
        print('Calling Textract JOB from EC2')
        # perform OCR and return balance sheet with corresponding page object(s)
        tb_response = readTable(res)           
        
        # checks for type of return, if none then we log an error
        if type(tb_response) == tuple:
            
            # deconstruct the table response tuple into dataframe and page object parts
            df, page_obj, page_num = tb_response
            print('\nPage number(s) for extraction in PNG are {}\n'.format(page_num))
            
            # provided balance sheet page number we select FORM and TEXT data
            forms_data = readForm(page_obj)      
            text_data = readText(page_obj)        
            
            print('\nTextract-PDF dataframe')
            print(df)
            
            print('\nFORMS Data')
            print(forms_data)
            
            print('\nTEXT Data')
            print(text_data)
        
        else:
            error = 'No Balance Sheet found, or parsing error'
            print(error)
    else:
        error = 'Could not parse, JOB FAILED'
        print(error)
        
if __name__ == '__main__':
    
    practice_main("ran-s3-systemic-risk", "Input/test.pdf")
          