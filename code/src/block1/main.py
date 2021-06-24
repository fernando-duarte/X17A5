#!/usr/bin/env python
# coding: utf-8


##################################
# INSTALL LIBRARIES
##################################

get_ipython().magic('conda update -n base -c defaults conda  # to update conda environment')
get_ipython().magic('conda install -c conda-forge poppler    # to install poppler PDF backend')

get_ipython().magic('pip install bs4')
get_ipython().magic('pip install PyPDF2')
get_ipython().magic('pip install pdf2image')
get_ipython().magic('pip install fitz')
get_ipython().magic('pip install pillow')
get_ipython().magic('pip install PyMuPDF==1.16.14')


##################################
# LIBRARY/PACKAGE IMPORTS
##################################

# interacting with Amazon AWS
import boto3
from sagemaker.session import Session

from pdf2image import convert_from_path
from ExtractBrokerDealers import dealerData
from FocusReportExtract import searchURL, edgarParse, fileExtract, mergePdfs
from FocusReportSlice import selectPages, extractSubset


##################################
# MAIN CODE EXECUTION
##################################

if __name__ == "__main__":
    
    # s3 active folders to interact with
    bucket = "ran-s3-systemic-risk"
    
    temp_folder ="Temp/"
    input_folder = 'Input/X-17A-5/'
    export_folder_pdf = "Input/X-17A-5-PDF-SUBSETS/"
    export_folder_png = "Input/X-17A-5-PNG-SUBSETS/"
    
    # Amazon Textract client and Sagemaker session
    s3 = boto3.client('s3')
    session = Session()
    
    # all s3 files corresponding within folders 
    temp_paths = session.list_s3_files(bucket, temp_folder)
    input_paths = session.list_s3_files(bucket, input_folder)
    file_paths = np.array(input_paths)[1:]
    
    pdf_paths = session.list_s3_files(bucket, export_folder_pdf)
    png_paths = session.list_s3_files(bucket, export_folder_png)
    
    # ==============================================================================
    #                 STEP 1 (Gathering updated broker-dealer list)
    # ==============================================================================
    
    # determine the interval range for which we look back historically 
    parse_years = np.arange(1993, datetime.datetime.today().year+1)

    if 'Temp/CIKandDealers.json' in temp_paths: 
        # retrieve old information from CIK and Dealers JSON file
        s3.download_file(bucket, 'Temp/CIKandDealers.json', 'temp.json')
        with open('temp.json', 'r') as f: old_cik2brokers = json.loads(f.read())
        
        # re-assign contents with new additional information 
        cik2brokers = dealerData(years=parse_years, cik2brokers=old_cik2brokers)   
        
        os.remove('temp.json')
    else:
        cik2brokers = dealerData(years=parse_years)
        
    # write to a JSON file with accompanying meta information about coverage 
    with open('CIKandDealers.json', 'w') as file:
        json.dump(cik2brokers, file)
        file.close()
    
    # save contents to AWS S3 bucket
    with open('CIKandDealers.json', 'rb') as data:
        s3.upload_fileobj(data, bucket, 'Temp/CIKandDealers.json')
    os.remove('CIKandDealers.json')
    
    print('\n===================\nStep 1: Gathering Broker-Dealer Data Completed\n===================\')
    
    # ==============================================================================
    #                 STEP 2 (Gathering X-17A-5 Filings)
    # ==============================================================================
    
    # full and partial broker dealer lists
    broker_dealers = cik2brokers['broker-dealers'].keys()
    select_dealers = [812291, 753835, 772028]
    
    for cik_id in select_dealers:
        companyName = cik2brokers['broker-dealers'][cik_id]
        
        # build lookup URLs to retrieve filing dates and archived url's
        url = searchURL(cik_id)
        response = edgarParse(url)
        
        if type(response) is not None:
            filing_dates, archives = response

            # logging info for when files are being downloaded
            print('Downloading X-17A-5 files for %s - CIK (%s)' % (companyName, cik_id))
            print('\t%s' % url)

            # itterate through each of the pdf URLs corresponding to archived contents
            for i, pdf_url in enumerate(archives):

                # construct filename & pdf file naming convention 
                date = filing_dates[i]                               # e.g. yyyy-MM-dd format
                file_name = str(cik_id) + '-' + date + '.pdf'        # e.g. 1904-2020-02-26.pdf 
                pdf_name = input_folder + file_name

                if pdf_name in input_paths: 
                    print('\tAll files for %s are downloaded' % companyName)
                    break

                else:
                    # extract all acompanying pdf files, merging all to one large pdf
                    pdf_files = fileExtract(pdf_url)
                    
                    if type(pdf_files) is not None:
                        concatPdf = mergePdfs(pdf_files)

                        # open file and save to local instance
                        with open(file_name, 'wb') as f:
                            concatPdf.write(f)
                            f.close()

                        # save contents to AWS S3 bucket
                        with open(file_name, 'rb') as data:
                            s3.upload_fileobj(data, bucket, pdf_name)
                        os.remove(file_name)
                    
                    else: print('\tNo files found for %s on %s' % (companyName, date))
        
        # identify error in the event edgar parse (web-scrapping returns None)
        else: print('ERROR: In downloading %s - CIK (%.d)' % (companyName, cik_id))
    
    
    print('\n===================\nStep 2: Gathering X-17A-5 Filings Completed\n===================\')
          
    # ==============================================================================
    #                 STEP 3 (Slice X-17A-5 Filings)
    # ==============================================================================
    
    for path_name in file_paths:
        print('Slicing information for %s' % path_name)
        
        # check to see if values are downloaded to s3 sub-bin
        base_file = path_name.split('/')[-1].split('.')[0]
        png_look_up = export_folder_png + base_file + '/' + base_file + '-p0.png'
        pdf_look_up = export_folder_pdf + base_file + '-subset.pdf'
        
        # ---------------------------------------------------------------
        # PDF FILE DOWNLOAD
        # ---------------------------------------------------------------
        
        if pdf_look_up not in pdf_paths:
            
            # retrieving downloaded files from s3 bucket
            s3.download_file(bucket, path_name, 'temp.pdf')
            
            # run the subset function to save a local subset file (void-function)
            export_name = base_file + '-subset.pdf'
            extractSubset([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14], export_name)
            
             # save contents to AWS S3 bucket as specified
            with open(export_name, 'rb') as data:
                s3.upload_fileobj(data, bucket, export_folder_pdf + export_name)
                print('\tSaved pdf files for -> %s' % export_name)
            
            # remove local file after it has been created
            os.remove('temp.pdf')
            os.remove(export_name)
            
        else:
            print('\t%s already saved pdf' % base_file)
        
        # ---------------------------------------------------------------
        # PNG FILE DOWNLOAD
        # ---------------------------------------------------------------
        
        if png_look_up not in png_paths:
            
            # retrieving downloaded files from s3 bucket
            s3.download_file(bucket, path_name, 'temp.pdf')
            
            try:
                # document class for temporary pdf (correspond to X-17A-5 pages)  
                pages = convert_from_path('temp.pdf', 500)
                
                # determine the iterable size (number of page in document)
                if len(pages) > 15:
                    size = 15
                else: size = len(pages)
                
                for idx in range(size):
                    # write the png name for exportation
                    export_file_name = "{}-p{}.png".format(base_file, idx)
                    
                    # storing PDF page as a PNG file locally (using pdf2image)
                    pages[idx].save(export_file_name, 'PNG')
                    
                    # save contents to AWS S3 bucket as specified
                    with open(export_file_name, 'rb') as data:
                        s3.upload_fileobj(data, bucket, export_folder_png + base_file + '/' + export_file_name)
                    
                    os.remove(export_file_name)
                    
                print('\tSaved png files for -> %s' % base_file)
                
                # remove local file after it has been created
                os.remove('temp.pdf')
                
            except PDFPageCountError:
                print('\tEncountered PDFPageCounterError when trying to convert to png for -> %s' % base_file)
            
        else:
            print('\t%s already saved png' % base_file)
     
    print('\n===================\nStep 3: Slicing X-17A-5 Filings Completed\n===================\')
          