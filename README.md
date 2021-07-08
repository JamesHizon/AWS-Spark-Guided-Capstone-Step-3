# AWS Spark Guided Capstone Step 3 - End-of-Day (EOD) Data Load

### Description
Now that I have preprocessed incoming data from the exchange, I need to create the final data format to store on the cloud. The cloud will also store historic exchange data, so Spring Capital can look up any trading day and easily find historic data.

At the end of the last step, I have created three partitions under an output directory. It is easy to go through them one by one and create corresponding datasets. Note that the target dataset should have the specific schema required by the partition.

### Learning Objectives
1. Create Spark DataFrames using Parquet files.
2. Perform data cleaning using Spark aggregation methods.
3. Use cloud storage as output of Spark jobs.

### Prerequisites
- PySpark: Read multiple Parquet files into a single DataFrame, transformations, write DataFrame.

### Steps
1. Read Trade Partition Dataset from Temporary Location (S3 Bucket in my case).
2. Select Necessary Columns for Trade Records.
3. Apply Data Correction.
4. Write the Trade Dataset back to Parquet on AWS S3.
5. Repeat Process for dealing with Quote dataset.

### Open Question:
- If I want to run SQL query against trade and quote data on AWS S3, I can use Spark to read the Parquet files and apply methods attached to Spark dataframe object such as ".select()", "groupBy()", and aggregation functions like ".agg(func.max())". I can also try to create a temporary view to obtain snapshot of our dataset.


