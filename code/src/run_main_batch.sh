#!/bin/bash

export https_proxy=http://p1proxy.frb.org:8080

# Batch file for running entire run_main.py script on both the
# EC2 and SageMaker instances, adjusting based on system

# check to see if the Anaconda distribution being requested is in directory
if [ ! -f "Anaconda3-2020.02-Linux-x86_64.sh" ]
then
    echo -e "Anaconda3-2020.02-Linux-x86_64.sh not found in directory, downloading...\n" 
    sudo bash
    export https_proxy=http://p1proxy.frb.org:8080

    # install conda for local use on the EC2 
    yum install python3 -y
    yum install libXcomposite libXcursor libXi libXtst libXrandr alsa-lib mesa-libEGL libXdamage mesa-libGL libXScrnSaver -y  
    wget https://repo.anaconda.com/archive/Anaconda3-2020.02-Linux-x86_64.sh  
    exit
else
    echo -e "Anaconda3-2020.02-Linux-x86_64.sh was found\n"
fi

export https_proxy=http://p1proxy.frb.org:8080
# check to see if the Anaconda directory exists in working home directory
if [ ! -d "/home/ec2-user/anaconda3" ]
then
    # execute Anaconda batch distribution 
    sh Anaconda3-2020.02-Linux-x86_64.sh -y
    export PATH=~/anaconda3/bin:$PATH
else
    echo -e "Anaconda directory already exists: /home/ec2-user/anaconda3\n"
fi

# #########################################################
# INSTALL LIBRARIES & DEPENDENCIES
# #########################################################

# update the conda update envrionment
conda update -n base -c defaults conda -y 

# downloading poppler backend to support pdf2image package
# conda install -c conda-forge poppler -y

pip3 install --upgrade pip
pip3 install bs4
pip3 install boto3==1.18.30 # more recent versions (now 1.20) won't work
pip3 install sagemaker
pip3 install PyPDF2
pip3 install pdf2image
pip3 install fitz
pip3 install pillow -y
pip3 install PyMuPDF==1.16.14
pip3 install smart_open
pip3 install minecart
pip3 install textract-trp
#conda install python-Levenshtein -y
pip3 install python-Levenshtein
pip3 install fuzzywuzzy
pip3 install joblib
pip3 install scikit-learn==0.24.1       # log-reg model stability verison
pip3 install pikepdf 
pip3 install aws
# #########################################################
# EXECUTING PRIMARY SCRIPT
# #########################################################

# run the main-script for the X-17A-5 project (run_main.py)
python3 run_main.py
