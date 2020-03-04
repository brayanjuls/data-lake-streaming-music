# Project Description
The intention of this project is to create a data lake to hold and process logs from a streaming music platform, the creation of this data lake will enable many kind of users to be able to perform analysis in the code, given that this is intented to be used for many users I created a star schema for bussiness analitycs users. 

# Steps to run the script 
The pre-requisites to run this project are:
    - Apache spark 2.4.3 or latest installed 
    - An AWS account 
    - An S3 bucket 
    - Python 2.7 or latest installed
First in the project you should navigate to the dl.cfg file and fill it with your AWS credentials and the path to your output s3 bucket without quotas, after that you should go to the console and navigate to the root of the project and execute the following command: `python etl.py`.  
    
# Files     
This repo contains 2 files and a small data set to test if you don't want to use AWS, 
the etl.py file is where the data is extracted from the source using schema-on-read, transformed using pyspark dataframes and load to the destination folder using parquet, the other file is dl.cfg which is where all the configurations of this script are saved.


    
    