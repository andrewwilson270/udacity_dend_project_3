import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1722596928952 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1722596928952")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1722598906176 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="AWSGlueDataCatalog_node1722598906176")

# Script generated for node Customer Privacy Filter JOIN
CustomerPrivacyFilterJOIN_node1722596498586 = Join.apply(frame1=AccelerometerLanding_node1722596928952, frame2=AWSGlueDataCatalog_node1722598906176, keys1=["user"], keys2=["email"], transformation_ctx="CustomerPrivacyFilterJOIN_node1722596498586")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1722596508531 = glueContext.getSink(path="s3://stedi-data-lake-house/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1722596508531")
AccelerometerTrusted_node1722596508531.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1722596508531.setFormat("json")
AccelerometerTrusted_node1722596508531.writeFrame(CustomerPrivacyFilterJOIN_node1722596498586)
job.commit()