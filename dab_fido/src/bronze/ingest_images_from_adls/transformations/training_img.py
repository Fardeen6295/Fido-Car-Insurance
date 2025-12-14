from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="training_images",
    comment="Raw Accident Images training data ingested from ADLS Gen2",
    table_properties={"quality":"bronze"}
)
def training_images():
    df = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format", "BINARYFILE")\
                .load("/Volumes/smart_claims_dev/00_landing/training_images")
    return df