import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
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

# Script generated for node Customer Curated
CustomerCurated_node1722666537584 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1722666537584")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1722666489254 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1722666489254")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1722666649783 = ApplyMapping.apply(frame=CustomerCurated_node1722666537584, mappings=[("serialnumber", "string", "right_serialnumber", "string")], transformation_ctx="RenamedkeysforJoin_node1722666649783")

# Script generated for node Filter data
StepTrainerLanding_node1722666489254DF = StepTrainerLanding_node1722666489254.toDF()
RenamedkeysforJoin_node1722666649783DF = RenamedkeysforJoin_node1722666649783.toDF()
Filterdata_node1722666641545 = DynamicFrame.fromDF(StepTrainerLanding_node1722666489254DF.join(RenamedkeysforJoin_node1722666649783DF, (StepTrainerLanding_node1722666489254DF['serialnumber'] == RenamedkeysforJoin_node1722666649783DF['right_serialnumber']), "left"), glueContext, "Filterdata_node1722666641545")

# Script generated for node Null filter
SqlQuery3510 = '''
select * from myDataSource
where right_serialnumber is not null
'''
Nullfilter_node1722667067778 = sparkSqlQuery(glueContext, query = SqlQuery3510, mapping = {"myDataSource":Filterdata_node1722666641545}, transformation_ctx = "Nullfilter_node1722667067778")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1722667193294 = glueContext.getSink(path="s3://stedi-data-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1722667193294")
StepTrainerTrusted_node1722667193294.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1722667193294.setFormat("json")
StepTrainerTrusted_node1722667193294.writeFrame(Nullfilter_node1722667067778)
job.commit()