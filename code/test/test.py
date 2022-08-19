import boto3
from datetime import date
from unittest import main, TestCase

today = date.today().strftime('%Y/%m/%d')
client = boto3.client('s3')
response_ex = client.list_objects_v2(Bucket='sb-de-c3', Prefix=f'raw/{today}/')
response_ld = client.list_objects_v2(Bucket='sb-de-c3', Prefix=f'transformed/{today}/')

class TestExtractLoad(TestCase):
    """
    TestExtractLoad

    Checks to see if the data was actually pulled and loaded into S3 correctly.

    Attributes
    ----------
    ex: list
        List of data files to be pulled from source.
    
    ld: list
        List of data files loaded into S3.
    """
    def __init__(self):
        self.ex = list(filter(lambda d: 'csv' in d, [content['Key'] for content in response_ex.get('Contents', [])]))
        self.ld = list(filter(lambda d: 'parquet' in d, \
            [content['Key'] for content in response_ld.get('Contents', [])])) 


    def test_extract(self):
        self.assertTrue(self.ex)

    def test_load(self):
        self.assertTrue(self.ld)


if __name__ == "__main__":
    main(verbosity=2)

