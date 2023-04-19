# Usage

```
   __________   ______                      ____                   
  / ___/__  /  /_  __/________ _____  _____/ __/___  _________ ___ 
  \__ \ /_ <    / / / ___/ __ `/ __ \/ ___/ /_/ __ \/ ___/ __ `__ \
 ___/ /__/ /   / / / /  / /_/ / / / (__  ) __/ /_/ / /  / / / / / /
/____/____/   /_/ /_/   \__,_/_/ /_/____/_/  \____/_/  /_/ /_/ /_/ 
                                                                   


S3 Transform v1.0.0 -- a tool to transform s3 objects

USAGE

  $ s3transform <command>

COMMANDS

  - setup <dynamodbTable>                                     Creates the dynamodb table if it does not exist
  - sample <dynamodbTable> <bucket> <integer>                 Generates sample data into the bucket
  - scan <bucket> <dynamodbTable>                             Scan S3 Folder and store results in dynamodb table
  - report <dynamodbTable> <output>                           Report transformation and exports results to S3 output
  - transform [(-i, --incremental)] <dynamodbTable> <output>  Transform files and copy them to S3 output bucket
  - reset-state                                               Reset copy state to false for all documents
```

# Local test with localstack

Start localstack & test manually: 
```
docker-compose up -d

# and then:
aws --endpoint http://localhost:4566 s3 mb s3://demo
aws --endpoint http://localhost:4566 s3 ls
aws --endpoint http://localhost:4566 s3 cp README.md s3://demo/
aws --endpoint http://localhost:4566 s3 cp README.md s3://demo/d1/
```

# What is the tool doing ?

Setup(dynamodb table)
- verify the source bucket exists
- create the dynamodb table if it doesn't exist

Scan(source bucket, dynamodb table)
- verify the source bucket exists
- verify the dynamodb table exists
- scan the source bucket -> write list of objects to dynamodb table

Transform(dynamodb table, transformation function)
- apply the transformation function to the files
- log the result
- state is managed in dynamodb, the process can be interrupted and resumed

Report(dynamodb table)
- display a statistics report of the transformations
