import abc
import boto3
import os

class Exporter(abc.ABC):
    @abc.abstractmethod
    def export(json_str):
        pass

class S3Exporter(Exporter):
    BUCKET = 'librivox-db'
    KEY = 'books.json'

    def __init__(self):
        pass

    def export(self, json_str):
        print(json_str)
        with open('books.json', 'w') as f:
            f.write(json_str)

        s3 = boto3.resource('s3',
                            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                            aws_secret_access_key=os.getenv(
                                'AWS_SECRET_ACCESS_KEY'))
        # s3 = boto3.resource('s3')
        response = s3.Bucket(self.BUCKET).upload_file('books.json', self.KEY)
        if response is None:
            print("Upload to S3 complete")
        else:
            print("Unable to upload to S3")
            print(response)