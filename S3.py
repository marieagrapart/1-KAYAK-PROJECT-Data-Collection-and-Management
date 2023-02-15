import boto3
import pandas as pd
import os

session = boto3.Session(aws_access_key_id=os.getenv("AWS_ACCESS_LEY_ID"), 
                        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"))


s3 = session.resource("s3")

bucket = s3.create_bucket(Bucket="booking-scapping")

df = pd.read_json('data/booking_test.json')
print(df.head())
csv = df.to_csv()


put_object = bucket.put_object(Key="hotels_info.csv", Body=csv)