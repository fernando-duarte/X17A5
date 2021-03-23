# X-17A-5 Optical Character Recognition (OCR)

## 1	Introduction
The project runs on Amazon Web Services (AWS) in a SageMaker instance. The scripts scrape the SEC for X-17A-5 filings and perform OCR through Amazon Textract, storing balance sheet information from files into s3 buckets. 

## 2	Software Dependencies
*	Python 3.6 (libraries boto3, bs4, textract-trp)

## 3	File Structure
### 3.1 	Resource Files
`secRegisteredDealers.txt` stores data downloaded from the [SEC](https://www.sec.gov/help/foiadocsbdfoiahtm.html). This ascii text file contains the Central Index Key (CIK) numbers, company names, SEC reporting file numbers, and addresses (business addresses are provided when mailing addresses are not available) of active broker-dealers who are registered with the SEC.

`secRegisteredDealers.csv` converts information from `secRegisteredDealers.txt` into a tab-delimited .csv file

`CIKandDealers.txt` JSON text file storing CIK numbers and company names as key/value pairs respectively 

### 3.2 	Code Files
`SEC_S3.ipynb` responsible for creating the resource files, downloading the X-17A-5 files from the SEC website and moving the downloaded files to a s3 buckets

`PDF_Slicing.ipynb` responsible for reducing the size of the X-17A-5 pdf files to mangeable ~9 page pdf(s)

`SEC_Textract.ipynb` calls the AWS asynchronous Textract API to perform OCR on the reduced X-17A-5 filings, selecting only the balance sheet and uploading it to a s3 bucket

## in progress ##
