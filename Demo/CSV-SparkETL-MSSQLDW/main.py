from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace, coalesce, lit, monotonically_increasing_id, year, month
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType

def create_spark_session():
    return SparkSession.builder \
        .appName("LoanDataProcessing") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-master:9000") \
        .getOrCreate()

def guess_column_types(spark, file_path):
    sample_df = spark.read.csv(file_path, header=True, inferSchema=True, samplingRatio=0.1)
    return sample_df.schema

def filter_columns(df, max_null_percentage=30):
    null_counts = df.select([((col(c).isNull().cast("int") / df.count()) * 100).alias(c) for c in df.columns])
    columns_to_keep = [c for c in null_counts.columns if null_counts.first()[c] <= max_null_percentage]
    return df.select(columns_to_keep)

def prepare_dataframe(df):
    return df.withColumn("issue_d", to_date(col("issue_d"), "MMM-yyyy")) \
             .withColumn("int_rate", regexp_replace(col("int_rate"), "%", "").cast(FloatType()) / 100) \
             .select([coalesce(col(c), lit("Unknown")).alias(c) if df.schema[c].dataType == StringType() else col(c) for c in df.columns])

def create_dimension_tables(df):
    home_ownership_dim = df.select("home_ownership").distinct() \
                           .withColumn("home_ownership_id", monotonically_increasing_id()) \
                           .withColumn("home_ownership", coalesce(col("home_ownership"), lit("Unknown")))

    loan_status_dim = df.select("loan_status").distinct() \
                        .withColumn("loan_status_id", monotonically_increasing_id()) \
                        .withColumn("loan_status", coalesce(col("loan_status"), lit("Unknown")))

    issue_d_dim = df.select("issue_d").distinct() \
                    .withColumn("issue_d_id", monotonically_increasing_id()) \
                    .withColumn("month", month("issue_d")) \
                    .withColumn("year", year("issue_d")) \
                    .na.drop()

    return home_ownership_dim, loan_status_dim, issue_d_dim

def create_fact_table(df, home_ownership_dim, loan_status_dim, issue_d_dim):
    loans_fact = df.join(home_ownership_dim, "home_ownership", "left") \
                   .join(loan_status_dim, "loan_status", "left") \
                   .join(issue_d_dim, "issue_d", "left")

    fact_columns = ["application_type", "loan_amnt", "funded_amnt", "term", "int_rate", "installment",
                    "home_ownership_id", "loan_status_id", "issue_d_id"]

    return loans_fact.select(fact_columns)

def save_to_database(spark, home_ownership_dim, loan_status_dim, issue_d_dim, loans_fact):
    jdbc_url = "jdbc:sqlserver://35.224.12.16:1433;databaseName=TestDB"
    connection_properties = {
        "user": "SA",
        "password": "Passw0rd123456",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }

    home_ownership_dim.write.jdbc(url=jdbc_url, table="home_ownership_dim", mode="overwrite", properties=connection_properties)
    loan_status_dim.write.jdbc(url=jdbc_url, table="loan_status_dim", mode="overwrite", properties=connection_properties)
    issue_d_dim.write.jdbc(url=jdbc_url, table="issue_d_dim", mode="overwrite", properties=connection_properties)
    loans_fact.write.jdbc(url=jdbc_url, table="loans_fact", mode="overwrite", properties=connection_properties)

def main():
    spark = create_spark_session()
    
    file_path = "hdfs://hadoop-master:9000/rawzone/Data/loan/LoanStats_web.csv"
    
    column_types = guess_column_types(spark, file_path)
    
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    
    filtered_df = filter_columns(df)
    
    prepared_df = prepare_dataframe(filtered_df)
    
    home_ownership_dim, loan_status_dim, issue_d_dim = create_dimension_tables(prepared_df)
    
    loans_fact = create_fact_table(prepared_df, home_ownership_dim, loan_status_dim, issue_d_dim)
    
    save_to_database(spark, home_ownership_dim, loan_status_dim, issue_d_dim, loans_fact)
    
    spark.stop()

if __name__ == "__main__":
    main()
