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

# Script generated for node Customer Trusted
CustomerTrusted_node1716894073703 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1716894073703")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1716894023552 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1716894023552")

# Script generated for node Join
Join_node1716892704934 = Join.apply(frame1=AccelerometerLanding_node1716894023552, frame2=CustomerTrusted_node1716894073703, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1716892704934")

# Script generated for node Drop Fields
SqlQuery0 = '''
select user, timestamp, x, y, z from JoinedTables
'''
DropFields_node1716923275239 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"JoinedTables":Join_node1716892704934}, transformation_ctx = "DropFields_node1716923275239")

# Script generated for node Drop Duplicates
DropDuplicates_node1716923138425 =  DynamicFrame.fromDF(DropFields_node1716923275239.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1716923138425")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1716892783781 = glueContext.getSink(path="s3://spark-demo-tam-bucket/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1716892783781")
AccelerometerTrusted_node1716892783781.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1716892783781.setFormat("json")
AccelerometerTrusted_node1716892783781.writeFrame(DropDuplicates_node1716923138425)
job.commit()