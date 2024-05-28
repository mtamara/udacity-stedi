import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1716924000011 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://spark-demo-tam-bucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1716924000011")

# Script generated for node Customer Trusted
CustomerTrusted_node1716923861932 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://spark-demo-tam-bucket/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1716923861932")

# Script generated for node Join
Join_node1716924031685 = Join.apply(frame1=AccelerometerTrusted_node1716924000011, frame2=CustomerTrusted_node1716923861932, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1716924031685")

# Script generated for node Drop Fields
DropFields_node1716924416302 = DropFields.apply(frame=Join_node1716924031685, paths=["z", "user", "y", "x", "timestamp"], transformation_ctx="DropFields_node1716924416302")

# Script generated for node Drop Duplicates
DropDuplicates_node1716924664948 =  DynamicFrame.fromDF(DropFields_node1716924416302.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1716924664948")

# Script generated for node Customer Curated
CustomerCurated_node1716924136322 = glueContext.getSink(path="s3://spark-demo-tam-bucket/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1716924136322")
CustomerCurated_node1716924136322.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1716924136322.setFormat("json")
CustomerCurated_node1716924136322.writeFrame(DropDuplicates_node1716924664948)
job.commit()