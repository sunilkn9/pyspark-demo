# main.py file

import sys
sys.path.append('../')
from logger import logger

from resources.data_layers import RawLayer, CleanedLayer, CuratedLayer, DataAggregation

def main_fun():
	try:
		spark = RawLayer.create_spark_session()
		logger.info("Raw Layer - Create Spark Session Completed")
		log_rdd = RawLayer.create_log_rdd(spark)
		logger.info("Raw Layer - Create Log RDD Completed")
		transformed_log_rdd = RawLayer.get_transformed_rdd(log_rdd)
		logger.info("Raw Layer - Get Transformed RDD Completed")
		log_df = RawLayer.create_log_df(spark, transformed_log_rdd)
		logger.info("Raw Layer - Create Log DataFrame Completed")
		RawLayer.save_df_internal(log_df)
		logger.info("Raw Layer - Create Log DataFrame Completed")
		RawLayer.load_df_to_s3()
		logger.info("Raw Layer - Load DataFrame to S3 Completed")
		RawLayer.create_hive_database(spark)
		logger.info("Raw Layer - Create HIVE DataBase Completed")
		RawLayer.save_raw_data_in_hive(log_df)
		logger.info("Raw Layer - Save Raw Data In HIVE Completed")

		cleaned_df = CleanedLayer.create_cleaned_df(log_df)
		logger.info("Cleaned Layer - Create Cleaned DataFrame Completed")
		CleanedLayer.save_df_internal(cleaned_df)
		logger.info("Cleaned Layer - Save DataFrame Internal Completed")
		CleanedLayer.load_cleansed_df_to_s3()
		logger.info("Cleaned Layer - Load Cleaned DataFrame to S3 Completed")
		CleanedLayer.save_cleansed_data_in_hive(cleaned_df)
		logger.info("Cleaned Layer - Save Cleaned Data In HIVE Completed")

		curated_df = CuratedLayer.create_curated_df(cleaned_df)
		logger.info("Curated Layer - Create Curated DataFrame Completed")
		CuratedLayer.save_df_internal(curated_df)
		logger.info("Curated Layer - Save DataFrame Internal Completed")
		CuratedLayer.load_curated_df_to_s3()
		logger.info("Curated Layer - Load Curated DataFrame to S3 Completed")
		CuratedLayer.save_curated_data_in_hive(curated_df)
		logger.info("Curated Layer - Save Curated Data In HIVE Completed")
		CuratedLayer.save_df_to_snowflake(curated_df)
		logger.info("Curated Layer - Save DataFrame to Snowflake Completed")

		per_device_df = DataAggregation.create_aggregate_per_device_df(curated_df)
		logger.info("Data Aggregation - Create Aggregate Per Device DataFrame Completed")
		DataAggregation.save_aggregate_per_device_df_in_hive(per_device_df)
		logger.info("Data Aggregation - Save Aggregate Per Device DataFrame In Hive Completed")
		across_devices = DataAggregation.create_aggregate_across_devices_df(curated_df)
		logger.info("Data Aggregation - Create Aggregate Across Device DataFrame Completed")
		DataAggregation.save_aggregate_across_devices_df_in_hive(across_devices)
		logger.info("Data Aggregation - Save Aggregate Across Devices DataFrame In Hive Completed")
	except:
		raise

if __name__ == "__main__":
	try:
		main_fun()
	except:
		raise