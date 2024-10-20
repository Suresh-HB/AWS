'''
@Author: Suresh 
@Date: 16-09-2024
@Last Modified time: 16-09-2024
@Title: Python program for performing CRUD operations using Boto3 with an S3 bucket.

'''


import boto3
from botocore.exceptions import ClientError
import os
from dotenv import load_dotenv
load_dotenv()

client = boto3.client("s3", region_name = os.getenv('AWS_REGION_NAME'))


def create_bucket(bucket_name):
    try:
        response = client.create_bucket(Bucket = bucket_name,
                                           CreateBucketConfiguration={
                                            'LocationConstraint':os.getenv('AWS_user_bucket_region')
                                           })
        return response
    except ClientError as e:
        print("Exception occured", e)


def list_all_buckets():
    response = client.list_buckets()
    for bucket in response['Buckets']:
        print(bucket["Name"])
    return response


def upload_file(file_path, bucket_name, object_name = None):
    if object_name is None:
        object_name =  file_path

    try:
        response = client.upload_file(file_path, bucket_name, object_name)

    except ClientError as e:
        print("error is", e)


def download_file(bucket_name, object_name, file_path): 
    response = client.download_file(bucket_name, object_name, file_path)


def delete_object(bucket_name, object_name):
    response = client.delete_object(Bucket = bucket_name, Key = object_name)


def delete_bucket(bucket_name):
    response = client.delete_bucket(
        Bucket = bucket_name
    )
    return response

def transfer_large_file(file_path, bucket_name):
    try:
        object_name = os.path.basename(file_path)
        response = client.upload_file(file_path, bucket_name, object_name)
        print(f"Large file '{file_path}' transferred successfully to '{bucket_name}/{object_name}'.")
    except ClientError as e:
        print("Error occurred while transferring large file:", e)


def main():

    bucket_name = "suresh-demo-bucket"
    response = create_bucket(bucket_name)
    
    if response:
        print(f"Bucket '{bucket_name}' created successfully.")
        print("Response:", response)
    else:
        print(f"Failed to create bucket '{bucket_name}'.") 
    
    all_buckets = list_all_buckets()
    print(all_buckets)

    file_path = r"C:\Users\Suresh\Desktop\pythonBl\Boto3CRUD\student.csv"
    large_file_path = r"C:\Users\Suresh\Desktop\pythonBl\Suresh-Book"

    bucket_name = 'suresh-demo-bucket'
    upload_file(file_path, bucket_name, 'student.csv')

    download_file("suresh-demo-bucket", "student.csv", "./aws_student")
    delete_object("suresh-demo-bucket", "student.csv")
    delete_bucket("suresh-demo-bucket")

    transfer_large_file(large_file_path, bucket_name)
   

if __name__ == '__main__':
    main()
