import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,from_json,window,avg
from pyspark.sql.types import StructType ,DoubleType ,StructField , StringType , TimestampType
from cassandra.cluster import Cluster



def create_spark_connection():

    try:
        spark = SparkSession.builder\
               .appName('stockdata')\
               .config('spark.jars.packages',"com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4")\
               .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")

    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")


    return spark


def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream\
                  .format('kafka')\
                  .option('kafka.bootstrap.servers','localhost:9092')\
                  .option('subscribe','apple_data')\
                  .option('startingOffsets','earliest')\
                  .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
          logging.warning(f"kafka dataframe not created:{e}")


    return spark_df

def extract_data_from_kafka(spark_df):
    schema = StructType([
         StructField("datetime",StringType(),False),
         StructField("open",DoubleType(),False),
         StructField("high",DoubleType(),False),
         StructField("low",DoubleType(),False),
         StructField("close",DoubleType(),False),
         StructField("close",DoubleType(),False)
    ]
    )


    sel = spark_df.selectExpr("CAST(value AS STRING")\
                  .select(from_json(col('value'),schema).alias('data'))\
                  .select("data.*")
    print(sel)

    return sel

def convert_time_to_date(pro_df):
    ww_df = pro_df.withColumn("timestamp",col("datetime").cast(TimestampType()))
    return ww_df

def tumble_window(tumble_df):
    result_df = tumble_df\
                .groupBy(window(col("timestamp"),"5 minutes"))\
                .agg(avg("close").alias("close"),
                     avg("open").alias("open"),
                     avg("volume").alias("volume"),
                     avg("high").alias("high"),
                     avg("low").alias("low"))\
                .select("window.start","window.end","close","open","volume","high","low")

    return result_df


def create_keyspace(session):
    session.execute(""" 
             CREATE KEYSPACE IF NOT EXISTS spark_streams
             WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':'1'};   
             """)
    print("keyspace created successfully!")

def create_table(session):
    session.execute("""
     CREATE TABLE IF NOT EXISTS spark_streams.created_users(
          window_start timestamp,
          window_end timestamp,
          close double,
          open  double,
          volume double,
          high   double,
          low    double,
          PRIMARY KEY (window_start)
     );
     """)

    print("table created successfully")


def insert_data(session,**kwargs):
    print("inserting data")

    window_start = kwargs.get('window_start')
    window_end = kwargs.get('window_end')
    close = kwargs.get('close')
    open = kwargs.get('open')
    volume = kwargs.get('volume')
    high = kwargs.get('high')
    low = kwargs.get('low')

    try:
        session.execute(""" 
            INSERT INTO spark_streams.created_users(window_start,window_end,
            close,open,volume,high,low)
            VALUES(%s,%s,%s,%s,%s,%s,%s)
        """,(window_start,window_end,close,open,volume,high,low))
        logging.info(f"data inserted")

    except Exception as e:
        logging.error(f'could not insert data')



def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"could not connect")
        return None


if __name__ == "__main__":
   session = None
   spark_conn = create_spark_connection()

if spark_conn is not None:
       spark_df = connect_to_kafka(spark_conn)
       selection_df = extract_data_from_kafka(spark_df)
       new1 = convert_time_to_date(selection_df)
       new2 = tumble_window(new1)


if session is not None:
        create_keyspace(session)
        create_table(session)

        logging.info("streaming is being started")

        streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                                  .option('checkpointLocation','')
                                  .outputMode('append')
                                  .option('keyspace','spark_streams')
                                  .option('table','created_users')
                                  .start())

        streaming_query.awaitTermination()









