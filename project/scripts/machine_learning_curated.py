import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node Step_trainer Trusted
Step_trainerTrusted_node1716926140551 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="Step_trainerTrusted_node1716926140551")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1716926159876 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1716926159876")

# Script generated for node Join
SqlQuery1640 = '''
select * from act join stt on act.timestamp = stt.sensorreadingtime;
'''
Join_node1716926215133 = sparkSqlQuery(glueContext, query = SqlQuery1640, mapping = {"act":AccelerometerTrusted_node1716926159876, "stt":Step_trainerTrusted_node1716926140551}, transformation_ctx = "Join_node1716926215133")

# Script generated for node Drop Duplicates
DropDuplicates_node1716926542579 =  DynamicFrame.fromDF(Join_node1716926215133.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1716926542579")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1716926471580 = glueContext.getSink(path="s3://spark-demo-tam-bucket/machine-learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1716926471580")
MachineLearningCurated_node1716926471580.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1716926471580.setFormat("json")
MachineLearningCurated_node1716926471580.writeFrame(DropDuplicates_node1716926542579)
job.commit()