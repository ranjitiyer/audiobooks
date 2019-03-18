import abc
import boto3
import os

class Exporter(abc.ABC):
    @abc.abstractmethod
    def export_books(json_str):
        pass

s3 = boto3.resource('s3',
            aws_access_key_id=os.getenv(
                'AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv(
                'AWS_SECRET_ACCESS_KEY'))

class S3Exporter(Exporter):
    BUCKET = 'librivox-db'

    # s3 file name (key)
    BOOKS_KEY = 'books.json'
    AUTHORS_KEY = 'authors.json'
    GENRES_KEY = 'genres.json'

    # type
    AUTHORS = 'authors'
    BOOKS = 'books'
    GENRES = 'genres'

    def __init__(self):
        pass

    def write_to_s3(self, json_str, kind):
        print(json_str)
        with open('staging.json', 'w') as f:
            f.write(json_str)

        if kind is self.BOOKS:
            key = self.BOOKS_KEY
        elif kind is self.GENRES:
            key = self.GENRES_KEY
        elif kind is self.AUTHORS:
            key = self.AUTHORS_KEY

        response = s3.Bucket(self.BUCKET).upload_file('staging.json', key)
        if response is None:
            print("Upload to S3 complete")
        else:
            print("Unable to upload to S3")
            print(response)

    def export_books(self, json_str):
        self.write_to_s3(json_str, self.BOOKS)

    def export_authors(self, json_str):
        self.write_to_s3(json_str, self.AUTHORS)

    def export_genres(self, json_str):
        self.write_to_s3(json_str, self.GENRES)
