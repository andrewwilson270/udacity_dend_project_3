import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Trusted
CustomerTrusted_node1722609596596 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1722609596596")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1722609746716 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1722609746716")

# Script generated for node Unique Customers
SqlQuery3291 = '''
select distinct user
from myDataSource
where user is not null
'''
UniqueCustomers_node1722619371756 = sparkSqlQuery(glueContext, query = SqlQuery3291, mapping = {"myDataSource":AccelerometerTrusted_node1722609746716}, transformation_ctx = "UniqueCustomers_node1722619371756")

# Script generated for node Join
Join_node1722611125696 = Join.apply(frame1=CustomerTrusted_node1722609596596, frame2=UniqueCustomers_node1722619371756, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1722611125696")

# Script generated for node Drop user field
Dropuserfield_node1722620866174 = DropFields.apply(frame=Join_node1722611125696, paths=["user"], transformation_ctx="Dropuserfield_node1722620866174")

# Script generated for node Customer Curated
CustomerCurated_node1722619493564 = glueContext.getSink(path="s3://stedi-data-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1722619493564")
CustomerCurated_node1722619493564.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1722619493564.setFormat("json")
CustomerCurated_node1722619493564.writeFrame(Dropuserfield_node1722620866174)
job.commit()