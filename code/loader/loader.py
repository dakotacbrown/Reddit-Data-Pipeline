from logger.logger import Log
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Load - World Bank').getOrCreate()                         # session created
spark.conf.set("spark.sql.shuffle.partitions", 4)                                               # partition management
log = Log(__name__)                                                                             # logger for project


class Loader:

    def __init__(self, today):
        self.URI = f's3a://Your Bucket Goes Here/Your Prefix Goes Here/{today}/'

    def load(self, frames):
        """
        load

        Loads the dataframes into S3 as parquet files

        Attributes
        ----------
        frames: pyspark dataframes that needs to be saved
        """

        frames[0].coalesce(1).write.mode('overwrite').parquet(f'{self.URI}user_table.parquet')
        frames[1].coalesce(1).write.mode('overwrite').parquet(f'{self.URI}color_table.parquet')
        frames[2].coalesce(1).write.mode('overwrite').parquet(f'{self.URI}coordinate_table.parquet')
        frames[3].coalesce(1).write.mode('overwrite').parquet(f'{self.URI}fact_table.parquet')
