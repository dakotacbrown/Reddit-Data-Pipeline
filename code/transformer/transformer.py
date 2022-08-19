from logger.logger import Log
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Transform - Reddit').getOrCreate()  # session created
spark.conf.set("spark.sql.shuffle.partitions", 4)  # partition management
log = Log(__name__)  # logger for project


class Transformer:
    def __init__(self):
        self.URI = 's3a://Your Bucket Goes Here/'
        self.schema = StructType([
                StructField('timestamp', TimestampType(), True),
                StructField('user_id', StringType(), True),
                StructField('pixel_color', StringType(), True),
                StructField('coordinate', StringType(), True),
        ])
        self.users = StructType([
            StructField('timestamp', TimestampType(), True),
            StructField('user_id', StringType(), True)
        ])
        self.colors = StructType([
            StructField('timestamp', TimestampType(), True),
            StructField('pixel_color', StringType(), True)
        ])
        self.coords = StructType([
            StructField('timestamp', TimestampType(), True),
            StructField('coordinate', StringType(), True)
        ])

    def csv_to_df(self, csv_file):
        """
        csv_to_df

        Opens each csv per folder and turns the first csv into a dataframe

        Attributes
        ----------
        csv_file: path of the directory holding the csv file

        Returns
        -------
        Dataframe stores information from a specific csv

        Acknowledgement:
        https://stackoverflow.com/questions/33503993/read-in-all-csv-files-from-a-directory-using-python
        """

        try:
            df = spark.read.option("header", True).csv(f'{self.URI}{csv_file}').cache()       # create rdd from CSV
            return df
        except Exception as e:
            log.logger.critical(e)
        finally:
            log.logger.info(f'Spark DataFrame Successfully created from CSV {csv_file}.')

    def fix2017(self, frame):
        """
        fix2017

        Transforms the place 2017 date to match the 2022 data.

        Parameters
        ----------
        frame: location holding the place 2017 data.

        Returns
        -------
        Cleaned pyspark Dataframe similar to the 2022 csv dataframes
        """
        try:
            frame = frame.withColumnRenamed('ts', 'timestamp_utc') \
                .withColumn('timestamp_utc', f.regexp_replace('timestamp_utc', r' UTC', '')) \
                .withColumn('timestamp_utc', f.to_timestamp('timestamp_utc')) \
                .withColumnRenamed('user_hash', 'user_id') \
                .withColumn('pixel_color', f.when(frame.color == 0, '#FFFFFF')
                            .when(frame.color == 1, '#E4E4E4')
                            .when(frame.color == 2, '#888888')
                            .when(frame.color == 3, '#222222')
                            .when(frame.color == 4, '#FFA7D1')
                            .when(frame.color == 5, '#E50000')
                            .when(frame.color == 6, '#E59500')
                            .when(frame.color == 7, '#A06A42')
                            .when(frame.color == 8, '#E5D900')
                            .when(frame.color == 9, '#94E044')
                            .when(frame.color == 10, '#02BE01')
                            .when(frame.color == 11, '#00E5F0')
                            .when(frame.color == 12, '#0083C7')
                            .when(frame.color == 13, '#0000EA')
                            .when(frame.color == 14, '#E04AFF')
                            .when(frame.color == 15, '#820080')).drop(frame.color) \
                .select('*', f.concat_ws(',', frame.x_coordinate, frame.y_coordinate)
                        .alias('coordinate')).drop(frame.x_coordinate).drop(frame.y_coordinate)
            return frame
        except Exception as e:
            log.logger.critical(e)
        finally:
            log.logger.info('Spark transformed timestamp columns.')

    def fix2022(self, frame):
        """
        fix2022

        Transforms the place 2022 timestamp data.

        Parameters
        ----------
        frame: location holding the place 2022 data.

        Returns
        -------
        Cleaned pyspark Dataframe column
        """

        try:
            frame = frame.withColumnRenamed('timestamp', 'timestamp_utc') \
                .withColumn('timestamp_utc', f.regexp_replace('timestamp_utc', r' UTC', '')) \
                .withColumn('timestamp_utc', f.to_timestamp('timestamp_utc'))
            return frame
        except Exception as e:
            log.logger.critical(e)
        finally:
            log.logger.info('Spark transformed timestamp column.')

    def transform(self, frames):
        """
        transform

        Transforms each dataframe into the correct format

        Attributes
        ----------
        frames: list of dataframes from the csv files

        Returns
        -------
        Transformed dataframes
        """

        try:
            df_user_id = frames[0].select(frames[0].timestamp_utc, frames[0].user_id).cache()
            df_colors = frames[0].select(frames[0].timestamp_utc, frames[0].pixel_color).cache()
            df_coordinate = frames[0].select(frames[0].timestamp_utc, frames[0].coordinate).cache()
            df_fact = frames[0].select('*').cache()
            for frame in frames[1:]:
                df_user_id = df_user_id.union(frame.select(frame.timestamp_utc, frame.user_id))
                df_colors = df_colors.union(frame.select(frame.timestamp_utc, frame.pixel_color))
                df_coordinate = df_coordinate.union(frame.select(frame.timestamp_utc, frame.coordinate))
                df_fact = df_fact.union(frame.select('*'))
            return df_user_id, df_colors, df_coordinate, df_fact
        except Exception as e:
            log.logger.critical(e)
        finally:
            log.logger.info('End Transform')
