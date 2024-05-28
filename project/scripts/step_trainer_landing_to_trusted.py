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

# Script generated for node Customer Curated
CustomerCurated_node1716925371873 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1716925371873")

# Script generated for node Step_trainer Landing
Step_trainerLanding_node1716925342916 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="Step_trainerLanding_node1716925342916")

# Script generated for node Join
SqlQuery2140 = '''
select * from cc join stl on cc.serialnumber = stl.serialnumber;
'''
Join_node1716921304127 = sparkSqlQuery(glueContext, query = SqlQuery2140, mapping = {"stl":Step_trainerLanding_node1716925342916, "cc":CustomerCurated_node1716925371873}, transformation_ctx = "Join_node1716921304127")

# Script generated for node Drop Fields
DropFields_node1716919712307 = DropFields.apply(frame=Join_node1716921304127, paths=["customerName", "email", "phone", "birthDay", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropFields_node1716919712307")

# Script generated for node Drop Duplicates
DropDuplicates_node1716921998023 =  DynamicFrame.fromDF(DropFields_node1716919712307.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1716921998023")

# Script generated for node Step_trainer Trusted
Step_trainerTrusted_node1716920099378 = glueContext.getSink(path="s3://spark-demo-tam-bucket/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="Step_trainerTrusted_node1716920099378")
Step_trainerTrusted_node1716920099378.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
Step_trainerTrusted_node1716920099378.setFormat("json")
Step_trainerTrusted_node1716920099378.writeFrame(DropDuplicates_node1716921998023)
job.commit()