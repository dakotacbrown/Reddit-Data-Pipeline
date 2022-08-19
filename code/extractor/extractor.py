import csv
import gzip
import boto3
import shutil
import requests
from logger.logger import Log

log = Log(__name__)                                                                             # logger for project


class Extractor:

    def __init__(self, today):
        self.client = boto3.Session().client('s3')
        self.resource = boto3.resource('s3')
        self.today = today
        self.S3_BUCKET = self.resource.Bucket('Your Bucket Goes Here')
        self.FOLDER = f'Your Prefix Goes Here/{today}/'

    def extract_csv(self, info):
        """
        Extract_CSV

        Pulls all the information from the source and saves to a new folder in S3.

        Attributes
        ----------
        info: Dict
            Dictionary of data files to be pulled.
        """

        log.logger.info(f'Extracting {info[0]}')
        try:
            response = requests.get(info[1])
            with open(f'/tmp/{info[0]}.csv', 'w+') as f:
                writer = csv.writer(f)
                for line in response.iter_lines():
                    writer.writerow(line.decode('utf-8').split(','))
            self.S3_BUCKET.upload_file(f'/tmp/{info[0]}.csv', f'{self.FOLDER}Your Prefix Goes Here/{info[0]}.csv')
        except Exception as e:
            log.logger.critical(e)
            exit()
        finally:
            log.logger.info(f'{info[0]} has been successfully extracted from source and loaded into S3')

    def extract_gzip(self, info):
        """
        Extract_CSV

        Pulls all the information from the source and saves to a new folder in S3.

        Attributes
        ----------
        info: Dict
            Dictionary of data files to be pulled.
        """

        log.logger.info(f'Extracting {info[0]}')
        try:
            response = requests.get(info[1], stream=True)
            if response.status_code == 200:
                with open(f'/tmp/{info[0]}.csv.gz', 'wb') as f:
                    f.write(response.raw.read())
                with gzip.open(f'/tmp/{info[0]}.csv.gz', 'rb') as f_in:
                    with open(f'/tmp/{info[0]}.csv', 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                self.S3_BUCKET.upload_file(f'/tmp/{info[0]}.csv', f'{self.FOLDER}Your Prefix Goes Here/{info[0]}.csv')


        except Exception as e:
            log.logger.critical(e)
            exit()
        finally:
            log.logger.info(f'{info[0]} has been successfully extracted from source and loaded into S3')
