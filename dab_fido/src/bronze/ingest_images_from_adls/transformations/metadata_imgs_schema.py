from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.table(
    name="claim_images_metadata",
    comment="Raw Accident Images Claim Metadata ingested from ADLS Gen2",
    table_properties={"quality":"bronze"}
)
def claim_images_metadata():
    df = spark.readStream.format("cloudFiles")\
            .option("cloudFiles.format", "csv")\
            .option("cloudFiles.schemaEvolutionMode", "rescue")\
            .load("/Volumes/smart_claims_dev/00_landing/claims/metadata/")
    return df