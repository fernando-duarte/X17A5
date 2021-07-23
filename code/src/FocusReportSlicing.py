#!/usr/bin/env python
# coding: utf-8

"""
FocusReportSlicing.py: Responsbile for reducing the size of FOCUS reports
to be easily read by AWS Textract, due to file constraints
"""

##################################
# LIBRARY/PACKAGE IMPORTS
##################################:

import numpy as np 

from pdf2image.exceptions import PDFPageCountError
from PyPDF2 import PdfFileReader, PdfFileWriter, utils


##################################
# USER DEFINED FUNCTIONS
##################################

def brokerFilter(select_list:list, directory_list:list):
    """
    Helps determine (filter) corresponding pdf strings from
    a list of filings present on the s3
    
    Parameters
    ----------
    select_list : list
        A list of CIKs that correspond to broker-dealers
            
    directory_list : list   
        An list of directories that store information pertaining
        to FOCUS report filings on the s3
    """
    
    # itterate through broker-dealers CIK and flag string presence in list
    for cik in select_list:
        if cik in directory_list:
            return True

def selectPages(pdf:PdfFileReader, pageSelection:list) -> PdfFileWriter:
    """
    Extracts pages from a pdf and returns a PdfFileWriter object 
    
    Parameters
    ----------
    pdf : PdfFileReader
        A PdfFileReader object that represents a pdf file 
        that has been read and interpreted
            
    pageSelection : list   
        The page numbers to be selected from the pdf. NOTE, 
        these page numbers do not have to be sequential, 
        but often times are read as such
    """
    
    # initialize a pdf object to store pdf pages
    pdfWriter = PdfFileWriter()
    nPages = pdf.getNumPages()

    # to manage pdfs that don't contain as many pages as listed  
    if nPages > max(pageSelection):
        
        # add the first n-pages from the document as specified in pageSelection 
        for page_num in pageSelection:
            pdfWriter.addPage(pdf.getPage(page_num))
            
        return pdfWriter
    
    else:     
        # add all pages from the document provided
        for page_num in np.arange(nPages):
            pdfWriter.addPage(pdf.getPage(page_num))
            
        return pdfWriter 

def extractSubset(pages:list, export_file:str):
    """
    Extracts a subset of pages from a pdf, provided the page
    numbers are specified by the user
    
    Parameters
    ----------
    pages : list
        A list of page numbers to extract from a given pdf 
        (e.g. [1, 2, 3, 4, 5, 6]) 
        
    export_file : str   
        The name for the pdf file to be exported, we traditional
        keep the orignal pdf name, with the accompanying subset 
        tag (e.g. 'CITI-2020-02-22-subset.pdf')
    """
    
    try:
        # read pdf file and initialize empty pdf file to create subset
        pdf = PdfFileReader('temp.pdf')
        subset = selectPages(pdf, pages)

        try:
            # open file and save to local instance
            with open(export_file, 'wb') as f:
                subset.write(f)
                f.close()
        except:
            print('Not able to save local file %s' % export_file)

    except utils.PdfReadError:
        print('EOF marker not found - reject %s' % export_file)
