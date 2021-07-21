#!/usr/bin/env python
# coding: utf-8

"""
RunPT_3.py: Script responsible for creating the structured database by 
aggregating individual b

    1) DatabaseSplits.py
    2) DatabaseUnstructured.py
    2) DatabaseStructured.py
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################

from sklearn.feature_extraction.text import HashingVectorizer


##################################
# MAIN CODE EXECUTION
##################################

def main_p3(s3_bucket, s3_pointer, s3_session):
    
    print('\n===================\nStep 6: Determined cutoffs to distinguished Assets from Liabilities & Equity\n===================\')
          
    print('\n===================\nStep 7: Unstructured Database has been Created\n===================\')
          
    print('\n===================\nStep 8: Structured Database has been Created\n===================\')
          