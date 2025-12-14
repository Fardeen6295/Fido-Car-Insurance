from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

catalog = "smart_claims_dev"
src_schema = "01_bronze"
trg_schema = "02_silver"

# Transforming Claims
expectations_claims = {
        "rule_1":"claim_no IS NOT NULL",
        "rule_2":"hour BETWEEN 0 AND 23"
}
@dp.table(
    name=f"{catalog}.{trg_schema}.claims",
    comment="cleaned Claims",
    table_properties={"quality":"silver"}
)
@dp.expect_all(expectations_claims)
def claims():
    df = dp.readStream(f"{catalog}.{src_schema}.claims")
    df = df.withColumn("license_issue_date", to_date(col("license_issue_date"), "dd-MM-yyyy"))\
            .drop("_rescued_data")
    return df

# Cleans Policies
expectations_policies={
    "rulw_1":"POLICY_NO IS NOT NULL"
}
@dp.table(
    name=f"{catalog}.{trg_schema}.policies",
    comment="Cleaned Policies",
    table_properties={"quality":"silver"}
)
@dp.expect_all(expectations_policies)
def policies():
    df = dp.readStream(f"{catalog}.{src_schema}.policies")
    df = df.withColumn("PREMIUM", abs("PREMIUM"))\
            .drop("_rescued_data")
    return df

# Clean Telematics
expectations_telematics={
    "rule_1":"latitude BETWEEN -90 AND 90",
    "rule_2":"longitude BETWEEN -180 AND 180"
}
@dp.table(
    name=f"{catalog}.{trg_schema}.telematics",
    comment="clean Telematics Data",
    table_properties={"quality":"silver"}
)
@dp.expect_all(expectations_telematics)
def telematics():
    df = dp.readStream(f"{catalog}.{src_schema}.telematics")
    df = df.withColumn("event_timestamp", to_timestamp(col("event_timestamp"), "yyyy-mm-dd HH:MM:ss"))\
            .drop("_rescued_data")
    return df


# Clean Customers
expectations_customers={
    "rule_1":"customer_id IS NOT NULL"
}
@dp.table(
    name=f"{catalog}.{trg_schema}.customers",
    comment="Cleaned Customers Table",
    table_properties={"quality":"silver"}
)
@dp.expect_all(expectations_customers)
def customers():
    df = dp.readStream(f"{catalog}.{src_schema}.customers")

    name_normalized = when(
        size(split(trim(col("name")), ",")) == 2,
        concat(
            initcap(trim(split(col("name"), ",").getItem(1))), lit(" "),
            initcap(trim(split(col("name"), ",").getItem(0)))
        )
    ).otherwise(initcap(trim(col("name"))))
    
    df = df.withColumn("date_of_birth", to_date(col("date_of_birth"), "dd-MM-yyyy"))\
            .withColumn("firstName", split(name_normalized, " ").getItem(0))\
            .withColumn("lastName", split(name_normalized, " ").getItem(1))\
            .withColumn("address", concat(col("BOROUGH"), lit(", "), col("zip_code")))\
            .drop("name", "_rescued_data")
    return df


# CLean Training Images

@dp.table(
    name=f"{catalog}.{trg_schema}.training_images",
    comment="Cleaned Training Images",
    table_properties={"quality":"silver"}
)
def training_images():
    df = dp.readStream(f"{catalog}.{src_schema}.training_images")
    df = df.withColumn("label", regexp_extract("path", r"/(\d+)-([a-zA-Z]+)(?: \(\d+\))?\.png$", 2))
    return df

# Clean Claim Images
@dp.table(
    name=f"{catalog}.{trg_schema}.claims_images",
    comment="Cleaned Claim Images",
    table_properties={"quality":"silver"}
)
def claims_images():
    df = dp.readStream(f"{catalog}.{src_schema}.claims_images")
    df = df.withColumn("image_name", regexp_extract(col("path"), r".*/(.*?.jpg)", 1))
    return df






