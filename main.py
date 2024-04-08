import boto3
import botocore.exceptions
import logging
from io import TextIOWrapper
from gzip import GzipFile
from smart_open import open
import sys

class Main:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            region_name='ap-northeast-1',
            endpoint_url='http://localhost:4566'
        )
        
        
    def execute(self):
        bucket_name = "test-bucket"
        object_name = "test.csv.gz"
        
        self._upload_object(bucket_name, object_name)
        
        size, stream = self.get_gzip_stream(bucket_name, object_name)
        print(size)
        print(sys.getsizeof(stream))

        counter = 0
        for line in stream:
            print(sys.getsizeof(stream))
            print(line)
            counter += 1
            if counter == 10:
                break

    def get_gzip_stream(self, bucket_name: str, object_name: str):
        stream_file = open(
            f's3://{bucket_name}/{object_name}', 
            'rb',
            transport_params = {'client': self.s3_client}
        )

        stream_file.seek(-4, 2)
        size = int.from_bytes(stream_file.read(), 'little')
        stream_file.seek(0)
        text_stream = TextIOWrapper(buffer=stream_file, encoding='utf-8')

        return size, text_stream


    def get_gzip_isize(self, bucket_name: str, object_name:str) -> int:
        with open(
            f's3://{bucket_name}/{object_name}', 
            'rb',
            transport_params = {'client': self.s3_client}
        ) as f:
            f.seek(-4, 2)
            size = int.from_bytes(f.read(), 'little')
            
            f.seek(0)
            text_stream = TextIOWrapper(buffer=f, encoding='utf-8')

            print(sys.getsizeof(f))
            
            return size, text_stream



        
    def _upload_object(self, bucket_name: str, object_name:str) -> None:
        try:
            self.s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={
                'LocationConstraint': 'ap-northeast-1'})
            # response を返さない
            self.s3_client.upload_file('test_file/test.csv.gz', bucket_name, object_name)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                return
            logging.error(e)
        

if __name__ == '__main__':
    main = Main()
    main.execute()
