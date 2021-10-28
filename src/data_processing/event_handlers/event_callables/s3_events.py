import os
import boto3
from botocore.exceptions import ClientError
from typing import Dict
from dotenv import load_dotenv
load_dotenv()

class S3:

    def __init__(self):
        self.client = boto3.Session().client('s3')
        self.bucket = os.getenv("aws_bucket")


    def upload(self, params):
        try:
            response = self.client.upload_file(params['file'], self.bucket, params['object_name'])
        except ClientError as e:
            params.log.error(e)
            return False
        return True


def upload_file_s3(event: str, params: Dict[str, str]):
    s3 = S3()
    s3.upload(params)