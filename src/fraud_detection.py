from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from hbase_dao import HBaseDao
from geo_map import GEO_Map
import time
import json

def process_transaction(row, hbase, geo):
    card_id = str(row['card_id']).encode()
    lookup = hbase.get_data(card_id, 'lookup_table')
    
    # Rule 1: UCL
    ucl = float(lookup.get(b'cf:ucl', 0))
    if row['amount'] > ucl:
        return 'fraud'
    
    # Rule 2: Credit Score
    credit_score = int(lookup.get(b'cf:credit_score', 0))
    if credit_score < 200:
        return 'fraud'
    
    # Rule 3: ZIP Code Distance
    last_postcode = lookup.get(b'cf:postcode', b'').decode()
    last_dt = lookup.get(b'cf:transaction_dt', b'').decode()
    if last_postcode and last_dt:
        lat1, lon1 = geo.get_lat(last_postcode), geo.get_long(last_postcode)
        lat2, lon2 = geo.get_lat(str(row['postcode'])), geo.get_long(str(row['postcode']))
        distance = geo.distance(lat1, lon1, lat2, lon2)
        time_diff = (time.mktime(time.strptime(row['transaction_dt'], "%d-%m-%Y %H:%M:%S")) - 
                     time.mktime(time.strptime(last_dt, "%d-%m-%Y %H:%M:%S"))) / 3600  # hours
        speed = distance / time_diff if time_diff > 0 else float('inf')
        if speed > 500:  # Arbitrary threshold (e.g., 500 km/h)
            return 'fraud'
    
    # Update lookup table if genuine
    data = {
        b'cf:postcode': str(row['postcode']).encode(),
        b'cf:transaction_dt': row['transaction_dt'].encode()
    }
    hbase.write_data(card_id, data, 'lookup_table')
    return 'genuine'

def main():
    spark = SparkSession.builder.appName("FraudDetection").getOrCreate()
    hbase = HBaseDao.get_instance()
    geo = GEO_Map.get_instance()

    # Kafka stream
    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
        .option("subscribe", "transactions-topic-verified") \
        .load()

    # Parse JSON
    schema = "card_id LONG, member_id LONG, amount DOUBLE, pos_id LONG, postcode STRING, transaction_dt STRING"
    parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), schema).alias("data")).select("data.*")

    # Process each transaction
    def foreach_batch_function(df, epoch_id):
        for row in df.collect():
            status = process_transaction(row.asDict(), hbase, geo)
            hbase.write_data(str(row['card_id']).encode(), 
                            {b'cf:status': status.encode()}, 
                            'card_transactions')
            print(f"Transaction {row['card_id']} classified as {status}")

    query = parsed_df.writeStream.foreachBatch(foreach_batch_function).start()
    query.awaitTermination()

if __name__ == "__main__":
    main()