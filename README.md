# X-17A-5 Optical Character Recognition (OCR)

## 1	Introduction
The project scrapes the SEC for X-17A-5 filings published by registred broker-dealers historically, and constructs a database for asset, liability and equity line items. The project runs on Amazon Web Services (AWS) in a SageMaker instance, and stores the scraped information  into an s3 bucket. 

## 2	Software Dependencies
* Python 3.6 (libraries: boto3, bs4, textract-trp, PyPDF2)

## 3	File Structure

### 3.1 	Resource Files

* `CIKandDealers.json` JSON file storing CIK numbers for firms and company names as key/value pairs respectively (e.g. {"356628": "NATIONAL FINANCIAL SERVICES LLC", "815855": "MERRILL LYNCH GOVERNMENT SECURITIES OF PUERTO RICO INC"}).

* `X17A5-forms.json` JSON file storing the CIK numbers with the accompanying FORMS data retrieved from Textract 

* `X17A5-text.json` JSON file storing the CIK numbers with the accompanying TEXT data retrieved from Textract

### 3.2 	Error Files

* `textractErrors.json` JSON file storing CIK numbers with accompanying year that were unable to be read via Textract. There are two types of errors that are raised:
    * *No Balance Sheet found, or parsing error*, where there may be an issue with Textract reading the page
    * *Could not parse, JOB FAILED*, where there may be an issue with Textract parsing the pdf file   
    
### 3.3 	Code Files

The code files are divided into four sub-groups that are responsible for executing a portion of the database construction. 

#### Broker-Dealer Gathering

   * `readDealerData.ipynb` responsible for creating the resource files, downloading the X-17A-5 files from the SEC website and moving the downloaded files to a s3 buckets

#### X17A5 File Retrieval

   * `pdfFileExtract.ipynb` responsible for extracting the X-17A-5 pdf files from broker-dealer URLS

   * `pdfFileSlicing.ipynb` responsible for reducing the size of the X-17A-5 pdf files to mangeable ~15 page pdf(s)

#### Optical Character Recognition

   * `ocrTextract.ipynb` calls the AWS asynchronous Textract API to perform OCR on the reduced X-17A-5 filings, selecting only the balance sheet and uploading it to a s3 bucket

   * `ocrClean.ipynb` refines the scraped balance sheet data from Textract, handling case exemptions such as merged rows, multi-year columns and numeric string conversions 

#### Database construcution

   * `databaseLineitems.ipynb` divides the balance sheet into asset terms and liability & equity terms

   * `databaseUnstructured.ipynb` constructs a semi-finished database that captures all unique line items by column for the entirety of each bank and year

   * `databaseStructured.ipynb` constructs a finished database that aggregates columns by a predicted class for assets and liability & equity terms

### 3.4 	Output Files

   * `assetLines.txt` & `liabilityLines.txt` both represent line items that correspond to asset and liability & equity terms respectively from all of the broker-dealers surveyed

   * `unstructAsset.csv` & `unstructLiable.csv` both represent the unstructured asset and liability & equity terms respectively, from all of the broker-dealers per year 

   * `structAsset.csv` & `structLiable.csv` both represent the structured asset and liability & equity terms respectively, from all of the broker-dealers per year 

## 4	Running Code

Our code file will run on AWS batch, via Sagemaker instance. We start by first cloning our Github [repository](https://github.com/Raj9898/X17A5) to a local AWS instance and ready a terminal for batch execution.   

### 4.1 	Setting up the Notebook Instance (First Time Use ONLY)

**Click the "Create notebook instance" button and modify the following:**

_**Notebook instance settings**_

 1. Notebook instance type: subject to user (e.g. ml.p2.xlarge)
 2. Lifecycle configuration: Use ran-lifecycle-config-julia-ver-1-4-0-gpu
    
_**Permissions and encryption**_

 1. IAM role ARN: For access to SageMaker commands and AWS features (e.g. ran-notebook-execution-role-fernandod)
 2. Root Access: Enabled (default) to give users root access to the notebook
 3. Encryption key: First select "Enter a KMS key ARN" from the drop-down menu and then provide the accompanying KMS ARN key below (i.e. arn:aws:kms:us-east-2:347550782223:key/acf48e27-89bf-4086-995d-ae42027ec4c0)

_**Network**_

1. Select the default provided vpc-efd69186 (172.31.0.0/16)
2. Subnets: Provide the us-east-ohio private region (i.e. subnet-036ca68944320a3d7)
3. Security Group(s): sg-02bc281a2eed08cdc
4. Direct interest access: Disabled, access the internet through  a VPC 

_**Git Repositories**_

If you would like to clone a Git repo, select "Clone a public Git repository to this notebook instance only" from the drop-down and proceed
 1. Git repository URL: If you have a repo provide the URL from GitHub

### 4.2 	Setting up the Terminal 

#### First Time Use ONLY

**Start the notebook and open in Jupyter. Click New -> terminal. Submit the following commands:**

1. ```aws s3 cp s3://ran-s3-install-pkgs/config/RanPocKP.pem /home/ec2-user/```

2. ```chmod 400 /home/ec2-user/RanPocKP.pem```

3. ```sudo ssh -i /home/ec2-user/RanPocKP.pem ec2-user@172.31.100.6```

#### Subsequent Use ONLY

**Start the notebook and open in Jupyter. Click New -> terminal. Submit the following commands:**

1. ```sudo su - ec2-user```
    
2. ```sudo ssh -i /home/ec2-user/RanPocKP.pem ec2-user@172.31.100.6```

### 4.3 	Using the Terminal 

**With an open terminal, submit the following commands**

1. Begin by transferring files to run from a local repository to a designating s3 bucket
   ```aws s3 cp (s3_repo_name) (home_file_path)```
   ```aws s3 cp s3://ran-s3-systemic-risk/Code/sample.py /home/ec2-user/sample.py```

2. To run the file and save a log-file to be viewed later. **(NOTE: CAN NOT BE USED FOR A JUPYTER NOTEBOOK file (.ipynb), ONLY PYTHON (.py) due to compiler restrictions)**
   ```nohup time python (home_file_path) > (file_name) &```
   ```nohup time python /home/ec2-user/sample.py > output.txt &```

3. To view the output files 
   ```view output_name.txt```

4. To exit the terminal window for view output window
   ```:qa!```

## 5	Possible Extensions
* Continue converting .ipynb files into .py files for use in batch execution
* Work on streamlining the runtime of each file via batch on AWS

## 6	Contributors
* [Rajesh Rao](https://github.com/Raj9898) (Sr. Research Analyst 22’)
