import boto3
import pandas as pd
import os
from sqlalchemy import create_engine
from botocore.exceptions import ClientError

# ENV variables
S3_BUCKET = os.getenv("S3_BUCKET")
S3_KEY = os.getenv("S3_KEY")

RDS_HOST = os.getenv("RDS_HOST")
RDS_USER = os.getenv("RDS_USER")
RDS_PASS = os.getenv("RDS_PASS")
RDS_DB = os.getenv("RDS_DB")
TABLE_NAME = os.getenv("TABLE_NAME")

GLUE_DB = os.getenv("GLUE_DB")
GLUE_TABLE = os.getenv("GLUE_TABLE")
S3_LOCATION = f"s3://{S3_BUCKET}/{S3_KEY}"

s3 = boto3.client('s3')
glue = boto3.client('glue')

def read_csv_from_s3():
    print("Reading CSV from S3...")

    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    df = pd.read_csv(obj['Body'])

    print(df.head())
    return df


def upload_to_rds(df):

    try:
        print("Connecting to RDS...")

        engine = create_engine(
    f"mysql+pymysql://{RDS_USER}:{RDS_PASS}@{RDS_HOST}/{RDS_DB}",
    connect_args={
        "ssl": {
            "ca": "/app/global-bundle.pem"
        }
    }
)

        df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)

        print("Data successfully inserted into RDS")

        return True

    except Exception as e:

        print("RDS upload failed:", e)

        return False


def fallback_to_glue():

    print("Falling back to AWS Glue")

    try:

        glue.create_table(
            DatabaseName=GLUE_DB,
            TableInput={
                'Name': GLUE_TABLE,
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'id', 'Type': 'int'},
                        {'Name': 'name', 'Type': 'string'},
                        {'Name': 'course', 'Type': 'string'},
                        {'Name': 'age', 'Type': 'int'}
                    ],
                    'Location': S3_LOCATION,
                    'InputFormat':
                    'org.apache.hadoop.mapred.TextInputFormat',
                    'OutputFormat':
                    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary':
                        'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )

        print("Glue table created successfully")

    except ClientError as e:

        print("Glue error:", e)


def main():

    df = read_csv_from_s3()

    success = upload_to_rds(df)

    if not success:

        fallback_to_glue()


if __name__ == "__main__":

    main()
