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
CustomerCurated_node1722670666418 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1722670666418")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1722670635471 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1722670635471")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1722670523530 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1722670523530")

# Script generated for node Customer Drop Fields
CustomerDropFields_node1722671903924 = DropFields.apply(frame=CustomerCurated_node1722670666418, paths=["sharewithpublicasofdate", "birthday", "sharewithresearchasofdate", "registrationdate", "customername", "sharewithfriendsasofdate", "email", "lastupdatedate", "phone"], transformation_ctx="CustomerDropFields_node1722671903924")

# Script generated for node Renamed keys for step trainer join acceleromter
Renamedkeysforsteptrainerjoinacceleromter_node1722671222104 = ApplyMapping.apply(frame=AccelerometerTrusted_node1722670635471, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("z", "double", "right_z", "double"), ("birthday", "string", "right_birthday", "string"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("registrationdate", "long", "right_registrationdate", "long"), ("customername", "string", "right_customername", "string"), ("user", "string", "right_user", "string"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"), ("y", "double", "right_y", "double"), ("x", "double", "right_x", "double"), ("timestamp", "long", "right_timestamp", "long"), ("email", "string", "right_email", "string"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("phone", "string", "right_phone", "string")], transformation_ctx="Renamedkeysforsteptrainerjoinacceleromter_node1722671222104")

# Script generated for node Renamed keys for Step Trainer join curstomer curated
RenamedkeysforStepTrainerjoincurstomercurated_node1722672063139 = ApplyMapping.apply(frame=CustomerDropFields_node1722671903924, mappings=[("serialNumber", "string", "right2_serialNumber", "string")], transformation_ctx="RenamedkeysforStepTrainerjoincurstomercurated_node1722672063139")

# Script generated for node step trainer join accelerometer
StepTrainerTrusted_node1722670523530DF = StepTrainerTrusted_node1722670523530.toDF()
Renamedkeysforsteptrainerjoinacceleromter_node1722671222104DF = Renamedkeysforsteptrainerjoinacceleromter_node1722671222104.toDF()
steptrainerjoinaccelerometer_node1722671172015 = DynamicFrame.fromDF(StepTrainerTrusted_node1722670523530DF.join(Renamedkeysforsteptrainerjoinacceleromter_node1722671222104DF, (StepTrainerTrusted_node1722670523530DF['sensorreadingtime'] == Renamedkeysforsteptrainerjoinacceleromter_node1722671222104DF['right_timestamp']), "left"), glueContext, "steptrainerjoinaccelerometer_node1722671172015")

# Script generated for node Step Trainer join curstomer curated
steptrainerjoinaccelerometer_node1722671172015DF = steptrainerjoinaccelerometer_node1722671172015.toDF()
RenamedkeysforStepTrainerjoincurstomercurated_node1722672063139DF = RenamedkeysforStepTrainerjoincurstomercurated_node1722672063139.toDF()
StepTrainerjoincurstomercurated_node1722671975296 = DynamicFrame.fromDF(steptrainerjoinaccelerometer_node1722671172015DF.join(RenamedkeysforStepTrainerjoincurstomercurated_node1722672063139DF, (steptrainerjoinaccelerometer_node1722671172015DF['right_serialnumber'] == RenamedkeysforStepTrainerjoincurstomercurated_node1722672063139DF['right2_serialNumber']), "left"), glueContext, "StepTrainerjoincurstomercurated_node1722671975296")

# Script generated for node filter for 
SqlQuery3956 = '''
select * from myDataSource
where right2_serialnumber is not null
'''
filterfor_node1722673940738 = sparkSqlQuery(glueContext, query = SqlQuery3956, mapping = {"myDataSource":StepTrainerjoincurstomercurated_node1722671975296}, transformation_ctx = "filterfor_node1722673940738")

# Script generated for node Drop Fields
DropFields_node1722672814623 = DropFields.apply(frame=filterfor_node1722673940738, paths=[], transformation_ctx="DropFields_node1722672814623")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1722672111803 = glueContext.getSink(path="s3://stedi-data-lake-house/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1722672111803")
MachineLearningCurated_node1722672111803.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1722672111803.setFormat("json")
MachineLearningCurated_node1722672111803.writeFrame(DropFields_node1722672814623)
job.commit()