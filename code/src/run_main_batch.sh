#!/bin/bash

# Batch file for running entire run_main.py script on both the
# EC2 and SageMaker

# user specified string for local storage of repo on SageMaker
sagemaker_path="/home/ec2-user/SageMaker/SEC_X17A5/code/src"

if [ $PWD == $sagemaker_path ]       

then
    
    echo -e '\nRunning shell-script on SageMaker Terminal\n\n'
    
    # update the conda environment 
    conda update -n base -c defaults conda -y 
    
    # downloading poppler backend to support pdf2image package
    conda install -c conda-forge poppler -y
    
    ####################################
    # INSTALL LIBRARIES (pip-install)
    ####################################
    pip install --upgrade pip
    pip install bs4
    pip install PyPDF2
    pip install pdf2image
    pip install fitz
    pip install pillow
    pip install PyMuPDF==1.16.14
    pip install smart_open
    pip install minecart
    pip install textract-trp
    pip install python-Levenshtein
    pip install fuzzywuzzy
    pip install joblib
    pip install scikit-learn==0.24.1    # log-reg model stability verison
    
    # run the main-script for the X-17A-5 project (run_main_focus.py)
    ipython run_main.py
    
else
    
    echo -e '\nRunning shell-script on EC2 Terminal\n\n'
    
    # install conda for local use on the EC2 
    sudo yum install python3 -y
    sudo yum install libXcomposite libXcursor libXi libXtst libXrandr alsa-lib mesa-libEGL libXdamage mesa-libGL libXScrnSaver -y  
    sudo wget https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh   
    sh Anaconda3-2020.02-Linux-x86_64.sh -y
    
    # create a new anaconda environment and update envrionment
    export PATH=~/anaconda3/bin:$PATH
    conda update -n base -c defaults conda -y 
    
    # downloading poppler backend to support pdf2image package
    conda install -c conda-forge poppler -y
    
    ####################################
    # INSTALL LIBRARIES (conda-install)
    ####################################
    pip3 install --upgrade pip
    pip3 install bs4
    pip3 install boto3
    pip3 install sagemaker
    pip3 install PyPDF2
    pip3 install pdf2image
    pip3 install fitz
    pip3 install pillow -y
    pip3 install PyMuPDF==1.16.14
    pip3 install smart_open
    pip3 install minecart
    pip3 install textract-trp
    conda install python-Levenshtein -y
    pip3 install fuzzywuzzy
    pip3 install joblib
    pip3 install scikit-learn==0.24.1       # log-reg model stability verison
    
    # run the main-script for the X-17A-5 project (run_main_focus.py)
    python3 run_main.py

fi