from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from hbase_dao import HBaseDao
from geo_map import GEO_Map
import time

def process_transaction(row, hbase, geo):
    card_id = str(row['card_id']).encode()
    lookup = hbase.get_data(card_id, 'lookup_table')
    
    if lookup is None:
        print(f"No lookup data found for card_id: {row['card_id']}, marking as fraud by default")
        return 'fraud'

    ucl = float(lookup.get(b'cf:ucl', b'0').decode())
    if row['amount'] > ucl:
        return 'fraud'
    
    credit_score = int(lookup.get(b'cf:credit_score', b'0').decode())
    if credit_score < 200:
        return 'fraud'
    
    last_postcode = lookup.get(b'cf:postcode', b'').decode()
    last_dt = lookup.get(b'cf:transaction_dt', b'').decode()
    if last_postcode and last_dt:
        lat1 = geo.get_lat(last_postcode)
        lon1 = geo.get_long(last_postcode)
        lat2 = geo.get_lat(str(row['postcode']))
        lon2 = geo.get_long(str(row['postcode']))
        
        if any(x is None for x in [lat1, lon1, lat2, lon2]):
            print(f"Warning: Invalid coordinates for postcode {last_postcode} or {row['postcode']}, skipping distance check")
        else:
            distance = geo.distance(lat1, lon1, lat2, lon2)
            time_diff = (time.mktime(time.strptime(row['transaction_dt'], "%d-%m-%Y %H:%M:%S")) - 
                        time.mktime(time.strptime(last_dt, "%d-%m-%Y %H:%M:%S"))) / 3600
            speed = distance / time_diff if time_diff > 0 else float('inf')
            if speed > 500:
                return 'fraud'
    
    data = {
        b'cf:postcode': str(row['postcode']).encode(),
        b'cf:transaction_dt': row['transaction_dt'].encode()
    }
    hbase.write_data(card_id, data, 'lookup_table')
    return 'genuine'

def main():
    spark = SparkSession.builder \
        .appName("FraudDetection") \
        .config("spark.jars", "/opt/spark-jars/spark-sql-kafka-0-10_2.12-3.2.0.jar,/opt/spark-jars/kafka-clients-2.8.0.jar,/opt/spark-jars/spark-streaming-kafka-0-10_2.12-3.2.0.jar,/opt/spark-jars/commons-pool2-2.11.1.jar,/opt/spark-jars/slf4j-api-1.7.30.jar,/opt/spark-jars/spark-token-provider-kafka-0-10_2.12-3.2.0.jar") \
        .getOrCreate()

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
        .option("subscribe", "transactions-topic-verified") \
        .option("kafka.fetch.max.wait.ms", "300000") \
        .option("kafka.request.timeout.ms", "300000") \
        .load()

    # Log dữ liệu thô từ Kafka
    raw_df = df.selectExpr("CAST(value AS STRING) as raw_value")
    raw_df.writeStream \
        .format("console") \
        .outputMode("append") \
        .start()

    schema = "card_id LONG, member_id LONG, amount DOUBLE, pos_id LONG, postcode STRING, transaction_dt STRING"
    parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

    def foreach_batch_function(df, epoch_id):
        def process_row(row):
            # Khởi tạo hbase và geo trong mỗi executor
            hbase = HBaseDao.get_instance()
            geo = GEO_Map.get_instance()
            row_dict = row.asDict()
            print(f"Received transaction: {row_dict}")
            if row_dict['card_id'] is None:
                print(f"Invalid transaction with card_id: None, skipping")
                return
            status = process_transaction(row_dict, hbase, geo)
            try:
                hbase.write_data(str(row_dict['card_id']).encode(), 
                                {b'cf:status': status.encode(),
                                 b'cf:member_id': str(row_dict['member_id']).encode(),
                                 b'cf:amount': str(row_dict['amount']).encode(),
                                 b'cf:postcode': str(row_dict['postcode']).encode(),
                                 b'cf:pos_id': str(row_dict['pos_id']).encode(),
                                 b'cf:transaction_dt': str(row_dict['transaction_dt']).encode()},
                                'card_transactions')
                print(f"Transaction {row_dict['card_id']} classified as {status} and written to HBase")
            except Exception as e:
                print(f"Failed to write transaction {row_dict['card_id']} to HBase: {e}")

        df.foreach(process_row)  # Xử lý từng hàng trong Spark

    query = parsed_df.writeStream.foreachBatch(foreach_batch_function).start()
    query.awaitTermination()

if __name__ == "__main__":
    main()