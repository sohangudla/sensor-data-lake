"""AWS Glue entry point (Glue 5.0 = Spark 3.5, Python 3.11).

The same `sensor_lake` package that runs locally is shipped to Glue as a
wheel via the job parameter --additional-python-modules (or --extra-py-files)
and called with an s3:// lake root. Only the SparkSession comes from Glue.

Job parameters:
  --LAKE_ROOT   s3://<bucket>/sensor-lake
Everything the job logs goes to CloudWatch Logs (/aws-glue/jobs/output|error);
key metrics are published to the CloudWatch namespace SensorLake.
"""
import os
import sys

from awsglue.context import GlueContext          # available only inside Glue
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from sensor_lake import pipeline

args = getResolvedOptions(sys.argv, ["JOB_NAME", "LAKE_ROOT"])
sc = SparkContext.getOrCreate()
glue = GlueContext(sc)
spark = glue.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.session.timeZone", "UTC")

job = Job(glue)
job.init(args["JOB_NAME"], args)
os.environ["SENSOR_LAKE_CLOUDWATCH"] = "1"
pipeline.run(spark, args["LAKE_ROOT"])   # raises -> Glue marks the run FAILED -> alarm
job.commit()
