# นำเข้าโมดูลที่จำเป็นจาก Apache Airflow และ Python standard library
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

# `default_args` เป็น dictionary ที่ระบุค่าพื้นฐานที่จะใช้ใน DAG นี้
default_args = {
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# สร้าง instance ของ DAG
dag = DAG('dag-loan-etl', default_args=default_args, schedule_interval=timedelta(minutes=10), catchup=False)

# สร้าง task สำหรับ ETL
etlTask = SparkSubmitOperator(
    task_id='etlTask',
    application="/home/hadoopuser/production/testforetl/main.py",
    conn_id="spark_default",
    packages="com.microsoft.azure:spark-mssql-connector:1.0.2,com.microsoft.sqlserver:mssql-jdbc:8.4.1.jre8",
    env_vars={
        "SPARK_HOME": "/home/hadoopuser/spark-3.0.0-bin-hadoop2.7",
        "PYSPARK_PYTHON": "/home/hadoopuser/anaconda3/bin/python"
    },
    dag=dag
)

# ไม่จำเป็นต้องกำหนดลำดับการทำงานเนื่องจากมีเพียง task เดียว
