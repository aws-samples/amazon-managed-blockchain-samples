# Token Dashboard

This mini-project makes use of Amazon Managed Blockchain Query to feed a Token Dashboard. 

Services used:
- Amazon Managed Blockchain Query
- AWS Glue
- Amazon Athena
- Amazon S3

## Installation

If running the scipts locally, install the local dependencies, using Python 3.x and `pip3`:

```
pip3 install pyspark
pip3 install git+https://github.com/awslabs/aws-glue-libs.git
python3 -c "from awsglue.utils import getResolvedOptions"
```

## Running the scripts

`token` and `s3_bucket_name` are mandatory fields.

```sh
# Mantle token
python token-transfers.py --token 0x3c3a81e81dc49a522a592e7622a7e711c06bf354 --s3_bucket_name xxxxxxxxxxxxxxxxxx

# USDT token
python token-transfers.py --token 0xdac17f958d2ee523a2206206994597c13d831ec7 --s3_bucket_name xxxxxxxxxxxxxxxxxx
```

<!-- "token","address","balance,"datetime" -->

## Steps to create the AWS Athena tables

From the data catalog of your choice, create a new database and run the two following queries. The table names are not strict to the solution, as long as they are properly selected from QuickSight. In the case of running the solution for multiple tokens, you can create one events and snapshot table per token, as it makes the QuickSight experience slightly better.

```sql
    CREATE EXTERNAL TABLE `events_token`(
        `contractaddress` string COMMENT 'from deserializer', 
        `eventtype` string COMMENT 'from deserializer', 
        `from` string COMMENT 'from deserializer', 
        `to` string COMMENT 'from deserializer', 
        `value` string COMMENT 'from deserializer', 
        `transactionhash` string COMMENT 'from deserializer', 
        `transactiontimestamp` timestamp COMMENT 'from deserializer'
    )
    ROW FORMAT SERDE 
    'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
    WITH SERDEPROPERTIES ( 
    'escapeChar'='\\', 
    'quoteChar'='\"', 
    'separatorChar'=',') 
    STORED AS INPUTFORMAT 
    'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
    's3://[YOUR_S3_BUCKET_NAME]/[TOKEN_ADDRESS]]/events/'
    TBLPROPERTIES ('classification'='csv', 'skip.header.line.count'='1')
```


```sql
    CREATE EXTERNAL TABLE `snapshot_token`(
        `token` string COMMENT 'from deserializer', 
        `address` string COMMENT 'from deserializer', 
        `balance` string COMMENT 'from deserializer',
        `last_updated_at` timestamp COMMENT 'from deserializer'
    )
    COMMENT 'Creating a snapshot table from Athena.'
    ROW FORMAT SERDE 
    'org.apache.hadoop.hive.serde2.OpenCSVSerde' 
    WITH SERDEPROPERTIES ( 
        'escapeChar'='\\', 
        'quoteChar'='\"', 
        'separatorChar'=','
    )
    STORED AS INPUTFORMAT 
    'org.apache.hadoop.mapred.TextInputFormat' 
    OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
    's3://[YOUR_S3_BUCKET_NAME]/[TOKEN_ADDRESS]]/snapshot/'
    TBLPROPERTIES ('classification'='csv', 'skip.header.line.count'='1')
```

