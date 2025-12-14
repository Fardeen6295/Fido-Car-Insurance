import geopy
import random
import pandas as pd
from typing import *
from pyspark.sql.types import *
from pyspark import pipelines as dp
from pyspark.sql.functions import *


def geocode(geolocator, address):
    try:
        #Skip the API call for faster demo (remove this line for ream)
        return pd.Series({'latitude':  random.uniform(-90, 90), 'longitude': random.uniform(-180, 180)})
        location = geolocator.geocode(address)
        if location:
            return pd.Series({'latitude': location.latitude, 'longitude': location.longitude})
    except Exception as e:
        print(f"error getting lat/long: {e}")
    return pd.Series({'latitude': None, 'longitude': None})
      
@pandas_udf("latitude float, longitude float")
def get_lat_long(batch_iter: Iterator[pd.Series]) -> Iterator[pd.DataFrame]:
  #ctx = ssl.create_default_context(cafile=certifi.where())
  #geopy.geocoders.options.default_ssl_context = ctx
  geolocator = geopy.Nominatim(user_agent="claim_lat_long", timeout=5, scheme='https')
  for address in batch_iter:
    yield address.apply(lambda x: geocode(geolocator, x))


catalog = "smart_claims_dev"
src_schema = '02_silver'
trg_schema = '03_gold'


# Aggregated Telematics Data MAT VIEW
@dp.table(
    name=f"{catalog}.{trg_schema}.aggregated_telematics",
    comment="Average of Telematics Data",
    table_properties={"quality":"gold"}
)
def aggregated_telematics():
    df = dp.read(f"{catalog}.{src_schema}.telematics")
    df = df.groupBy("chassis_no").agg(
        avg("speed").alias("telematics_avg_speed"),
        avg("latitude").alias("telematics_latitude"),
        avg("longitude").alias("telematics_longitude")
    )
    return df

# Customers Claims Policies Joined Streaming Table

@dp.table(
    name=f"{catalog}.{trg_schema}.customers_claims_policies",
    comment="Joined Table of Customers, Claims and Policies",
    table_properties={"quality":"gold"}
)
def customers_claims_policies():
    customers = dp.readStream(f"{catalog}.{src_schema}.customers")
    customers = customers.drop("created_at")
    policies = dp.readStream(f"{catalog}.{src_schema}.policies")
    policies = policies.drop("created_at")
    claims = dp.readStream(f"{catalog}.{src_schema}.claims")
    claims = claims.drop("created_at")
    claims_policies = claims.join(policies, "policy_no")
    claims_policies_customers = claims_policies.join(customers, claims_policies.CUST_ID == customers.customer_id)
    return claims_policies_customers

# Customers Claims Policies Telematics Aggregated Joined Mat View

@dp.table(
    name=f"{catalog}.{trg_schema}.customers_claims_policies_telematics",
    comment="claims with Goe Location latitude/longitude",
    table_properties={"quality":"gold"}
)
def customers_claims_policies_telematics():
    telematics = dp.read(f"{catalog}.{trg_schema}.aggregated_telematics")
    cust_clam_pol = dp.read(f"{catalog}.{trg_schema}.customers_claims_policies").where("BOROUGH IS NOT NULL")
    cust_clam_pol = cust_clam_pol.withColumnRenamed("CHASSIS_NO", "chassis_num")
    cust_clam_pol = cust_clam_pol.withColumn("lat_long", get_lat_long(col("address")))\
        .join(telematics, telematics.chassis_no == cust_clam_pol.chassis_num)
    return cust_clam_pol





