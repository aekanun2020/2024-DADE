import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, regexp_replace, coalesce, lit, year, month, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

# กำหนดการแสดงผล logging
logging.basicConfig(level=logging.ERROR)

def create_spark_session():
    spark = SparkSession.builder \
        .appName("LoanDataProcessing") \
        .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-master:9000") \
        .getOrCreate()
    
    # ตั้งค่า log level ของ Spark ให้แสดงเฉพาะ ERROR ขึ้นไป
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark

def guess_column_types(spark, file_path):
    sample_df = spark.read.csv(file_path, header=True, inferSchema=True, samplingRatio=0.1)
    return sample_df.schema

def filter_columns(df, max_null_percentage=30):
    total_count = df.count()
    
    null_percentages = {c: (df.filter(col(c).isNull()).count() / total_count) * 100 for c in df.columns}
    columns_to_keep = [c for c, null_percentage in null_percentages.items() if null_percentage <= max_null_percentage]
    
    columns_removed = set(df.columns) - set(columns_to_keep)
    print(f"Columns removed due to high null percentage: {columns_removed}")
    
    return df.select(columns_to_keep), columns_to_keep

def prepare_dataframe(df):
    # แปลง issue_d เป็น DateType
    df = df.withColumn("issue_d", to_date(col("issue_d"), "MMM-yyyy"))
    
    # แปลง int_rate เป็น FloatType: ลบเครื่องหมาย % และแปลงเป็น float
    df = df.withColumn("int_rate", regexp_replace(col("int_rate"), "%", "").cast(FloatType()) / 100)
    
    # แปลง loan_amnt และ funded_amnt เป็น IntegerType
    df = df.withColumn("loan_amnt", col("loan_amnt").cast("integer"))
    df = df.withColumn("funded_amnt", col("funded_amnt").cast("integer"))
    
    # แปลง installment เป็น FloatType
    df = df.withColumn("installment", col("installment").cast("float"))
    
    return df

def create_dimension_tables(df):
    window_spec = Window.orderBy("home_ownership")
    home_ownership_dim = df.select("home_ownership").distinct() \
                           .withColumn("home_ownership_id", row_number().over(window_spec)) \
                           .withColumn("home_ownership", coalesce(col("home_ownership"), lit("Unknown")))

    window_spec = Window.orderBy("loan_status")
    loan_status_dim = df.select("loan_status").distinct() \
                        .withColumn("loan_status_id", row_number().over(window_spec)) \
                        .withColumn("loan_status", coalesce(col("loan_status"), lit("Unknown")))

    window_spec = Window.orderBy("issue_d")
    issue_d_dim = df.select("issue_d").distinct() \
                    .withColumn("issue_d_id", row_number().over(window_spec)) \
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

    # แปลง ID columns เป็น IntegerType
    for id_col in ["home_ownership_id", "loan_status_id", "issue_d_id"]:
        loans_fact = loans_fact.withColumn(id_col, col(id_col).cast("integer"))

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

def inspect_data(df):
    print(f"Total columns before filtering: {len(df.columns)}")
    
    total_count = df.count()
    null_percentages = {c: (df.filter(col(c).isNull()).count() / total_count) * 100 for c in df.columns}
    
    print("Null percentages for each column:")
    for column, percentage in null_percentages.items():
        print(f"{column}: {percentage:.2f}%")

def drop_null_rows(df, threshold=None):
    if threshold:
        # ลบแถวที่มีค่า null มากกว่าหรือเท่ากับ threshold
        return df.dropna(thresh=threshold)
    else:
        # ลบแถวที่มีค่า null อย่างน้อยหนึ่งค่า
        return df.dropna()

def main():
    try:
        spark = create_spark_session()
        
        file_path = "hdfs://hadoop-master:9000/rawzone/Data/loan/LoanStats_web.csv"
        
        column_types = guess_column_types(spark, file_path)
        
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        
        inspect_data(df)
        
        filtered_df, remaining_columns = filter_columns(df)
        
        print(f"Total columns after filtering: {len(filtered_df.columns)}")
        print("Remaining columns after filtering:")
        for column in remaining_columns:
            print(column)
        
        prepared_df = prepare_dataframe(filtered_df)
        
        # เพิ่มการใช้ drop_null_rows
        rows_before = prepared_df.count()
        prepared_df = drop_null_rows(prepared_df, threshold=len(prepared_df.columns) - 5)  # ลบแถวที่มีค่า null มากกว่า 5 คอลัมน์
        rows_after = prepared_df.count()
        print(f"Rows before dropping null values: {rows_before}")
        print(f"Rows after dropping null values: {rows_after}")
        print(f"Rows removed: {rows_before - rows_after}")
        
        home_ownership_dim, loan_status_dim, issue_d_dim = create_dimension_tables(prepared_df)
        
        loans_fact = create_fact_table(prepared_df, home_ownership_dim, loan_status_dim, issue_d_dim)
        
        save_to_database(spark, home_ownership_dim, loan_status_dim, issue_d_dim, loans_fact)
        
        print("Data processing and saving completed successfully.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
