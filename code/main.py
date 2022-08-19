import boto3
import concurrent.futures
from datetime import date
from loader.loader import Loader
from logger.logger import Log
from extractor.extractor import Extractor
from transformer.transformer import Transformer

log = Log(__name__)                                                                             # logger for project

log.logger.info('ELTL Starting')

today = date.today().strftime('%Y/%m/%d')
client = boto3.client('s3')
response = client.list_objects_v2(Bucket='Your Bucket Goes Here', Prefix=f'Your Prefix Goes Here/{today}/')

extract = Extractor(today)
transform = Transformer()
load = Loader(today)

keys = [f"{a:02}" for a in range(0, 79)]
values = [f"https://placedata.reddit.com/data/canvas-history/2022_place_canvas_history-0000000000{a:02}.csv.gzip" for a
          in range(0, 79)]

place2022 = dict(zip(keys, values))
place2017 = {"Place2017": "https://storage.googleapis.com/place_data_share/place_tiles.csv"}

with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(extract.extract_csv, place2017.items())
    executor.map(extract.extract_gzip, place2022.items())

with concurrent.futures.ThreadPoolExecutor() as executor2:
    directories = list(filter(lambda d: 'csv' in d, [content['Key'] for content in response.get('Contents', [])]))
    rawFrames = list(executor2.map(transform.csv_to_df, directories))

with concurrent.futures.ThreadPoolExecutor() as executor3:
    df_2017 = transform.fix2017(rawFrames[0])
    df_list = [item for item in rawFrames[1:]]
    df_2022 = list(executor3.map(transform.fix2022, df_list))
    raw_transformed = [df_2017] + df_2022

transformedFrames = transform.transform(raw_transformed)
load.load(transformedFrames)
