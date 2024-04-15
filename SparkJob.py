import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("S3ReadJob") \
        .getOrCreate()

    try:
        logger.info("Starting Spark job")

        # Get UUID from command-line argument
        job_uuid = sys.argv[1] if len(sys.argv) > 1 else None
        state=sys.argv[2]
    

        if job_uuid is None:
            raise ValueError("UUID not provided as command-line argument")

        input_path = "s3://practiseprojcet/output/large.parquet"

        logger.info("Reading data from S3")
      
      
        df = spark.read.option("header", "true").parquet(input_path)
        
        df = df.repartition(4)

        filterd_data=df.filter(f"State='{state}'")

        # Define output directory using UUID
        output_path = f"s3://practiseprojcet/Output1/{job_uuid}"

        # Write DataFrame to Parquet format on S3 with UUID as part of directory name
        filterd_data.coalesce(1).write.mode("append").option("header","true").csv(output_path)

        logger.info("Spark job completed successfully")

    except Exception as e:
        # Log any exceptions that occur
        logger.error("An error occurred: %s", str(e))
        raise e

    finally:
        # Stop the SparkSession
        spark.stop()
