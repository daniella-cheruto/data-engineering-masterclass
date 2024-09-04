## Data Engineering on AWS Masterclass
The objectives
The goal of the masterclass was to leverage AWS for complete data engineering workflow. The project involved extracting data from the Rick and Morty API, applying transformations, and loading the data into a MySQL database on AWS RDS
# Learning objectives
1. How to design a real-world data platform on AWS
2. The creation of data flow diagrams
3. The usage of AWS Lambda for data ingestion
4. The staging of data using AWS S3
5. The building of a MYSQL data warehouse with AWS RDS
6. The automation process using AWS EventBridge
6. The running of SQL queries for analytics

# Data Ingestion
# Working with AWS Lambda
1. The Creation of a lambda function
In the AWS Console, I created a lambda function using Python 3.12. Afterwards, I added a layer for Pandas by selecting AWSSDKPandas-Python311. The timeout was increased to 5 minutes, and an AmazonS3FullAccess policy was attached to grant Lambda access to the S3 bucket.
2. Python Script for S3 Operations
I created 's3_file_operations.py' to handle writing data to S3 bucket

# The creation of an S3 bucket
In the AWS console, I created a new bucket with a unique name. for example the name of my bucket was de-masterclass-mutai. A The bucket was configured by disabling the ACLs and blocking public access. Versioning and encryption (SSE-S3) were enabled.

# Creating AWS RDS instance
In the AWS console, I created a MYSQL RDS instance(Free Tier). Furthermore, in configuration, we set the instance identifier to 'rick-and-morty-db'. In addition, I used secure credentials for master username and password. Furthermore, I edited the security group to allow MYSQL connections(port 3306) from the IP address

# Connecting everything together
1. Lambda functions
Extraction Function: I pulled data from the API and saved it to s3
Transformation Function : I read raw data from s3, applied transformations and saved the output back to s3.
Loading Function: I loaded the transformed data from s3 into RDS

# Testing of the pipeline
I tested each Lambda function to ensure proper data flow and transformation.

Through the following of the above steps, a functional data pipeline was set up on AWS, covering data extraction, transformation and analytics

