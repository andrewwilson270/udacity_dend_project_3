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

# Script generated for node Customer Landing
CustomerLanding_node1722452513513 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-house/customer/landing/"], "recurse": True}, transformation_ctx="CustomerLanding_node1722452513513")

# Script generated for node Share with Research
SqlQuery2625 = '''
select * from myDataSource
where shareWithResearchAsOfDate is not null
'''
SharewithResearch_node1722451913415 = sparkSqlQuery(glueContext, query = SqlQuery2625, mapping = {"myDataSource":CustomerLanding_node1722452513513}, transformation_ctx = "SharewithResearch_node1722451913415")

# Script generated for node Customer Trusted
CustomerTrusted_node1722452238929 = glueContext.getSink(path="s3://stedi-data-lake-house/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1722452238929")
CustomerTrusted_node1722452238929.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1722452238929.setFormat("json")
CustomerTrusted_node1722452238929.writeFrame(SharewithResearch_node1722451913415)
job.commit()