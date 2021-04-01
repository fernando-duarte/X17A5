"""
PDF Slicing for X-17A-5 Files

We slice the first 15 pages from the merged X-17A-5 files retrieved from the SEC 

@author: Rajesh Rao (Sr. Research Analyst 22')
"""

# ----------------------------------------------------------
# Package Imports
# ----------------------------------------------------------

import os
import boto3
import numpy as np 

from sagemaker.session import Session
from PyPDF2 import PdfFileReader, PdfFileWriter, utils

# ----------------------------------------------------------
# Function declarations
# ----------------------------------------------------------

def selectPages(pdf:PdfFileReader, pageSelection:list) -> PdfFileWriter:
    """
    Extracts pages from a pdf and returns a PdfFileWriter object 
    
    Input:
        :param: pdf (type PdfFileReader)
            A PdfFileReader object that represents a pdf file that has been read and interpreted
        :param: pageSelection (type list)   
            The page numbers to be selected from the pdf. NOTE, these page numbers do not hav to be sequential, 
            but often times are read as such
    Return:
        :param: pdfWriter (type PdfFileWriter)
            Returns a truncated PdfFile object that is smaller than or equal to the original parsed pdf
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
    Extracts a subset of pages from a pdf, provided the page numbers are specified
    
    Input:
        :param: pages (type list)
            A list of page numbers to extract from a given pdf (e.g. [1, 2, 3, 4, 5, 6]) 
        :param: export_file (type str)   
            The name for the pdf file to be exported, we traditional keep the orignal pdf name, with the 
            accompanying subset tag (e.g. 'CITI-2020-02-22-subset.pdf')
    Return:
        This is a void function, we return no value(s) as we interface with AWS s3 bucket to store pdfs
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
            print('Not able to save local file {}'.format(export_file))

    except utils.PdfReadError:
        print('EOF marker not found - reject {}'.format(export_file))
        
# ----------------------------------------------------------
# Main Script
# ----------------------------------------------------------

if __name__ == "__main__":
    
    bucket = "ran-s3-systemic-risk"
    import_folder = 'Input/X-17A-5/'
    export_folder = "Input/X-17A-5-Subsets/"

    # Amazon Textract client and Sagemaker session
    textract = boto3.client('textract')
    s3 = boto3.client('s3')
    session = Session()
    
    # pages to keep from each pdf
    pages = np.arange(15) 
    
    # import paths for all the subset files 
    # (NOTE: we take the 1+ index, since the the zero position is folder directory)
    importPaths = np.array(session.list_s3_files(bucket, import_folder))[1:]

    # export file paths to document subfolder
    exportPaths = session.list_s3_files(bucket, export_folder)

    for pdf_file_path in importPaths:

        # check to see if values are downloaded to s3 sub-bin
        baseFile = pdf_file_path.split('/')[-1].split('.')[0]
        export_file = '{}-subset.pdf'.format(baseFile)

        # if our subset is not found in our s3 bucket we look to extract it 
        if export_folder + exportFile not in exportPaths:

            # retrieving downloaded files from s3 bucket
            s3.download_file(bucket, pdf_file_path, 'temp.pdf')
            
            # run the subset function to save a local subset file (void-function)
            extractSubset(pages, export_file)
            
            # save contents to AWS S3 bucket as specified
            with open(export_file, 'rb') as data:
                s3.upload_fileobj(data, bucket, export_folder + export_file)
                
            print('Saved file -> {}'.format(export_file))
            
            # remove local file after it has been created
            os.remove(export_file)
            os.remove('temp.pdf')
            
        else:
            print('{} already saved'.format(exportFile))