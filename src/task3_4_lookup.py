from pyspark.sql import SparkSession
from hbase_dao import HBaseDao

def create_and_populate_lookup():
    # Khởi tạo Spark session
    spark = SparkSession.builder \
        .appName("LookupTablePopulation") \
        .config("spark.jars", "/opt/spark-jars/hbase-spark-1.4.7.jar").getOrCreate()

    # Đọc dữ liệu từ HBase (giả sử bảng card_member và member_score đã tồn tại)
    member_data = spark.read.format("org.apache.hadoop.hbase.spark") \
        .option("hbase.table", "card_member") \
        .option("hbase.columns.mapping", "cf:card_id=card_id,cf:member_id=member_id,cf:postcode=postcode") \
        .load()

    member_scores = spark.read.format("org.apache.hadoop.hbase.spark") \
        .option("hbase.table", "member_score") \
        .option("hbase.columns.mapping", "cf:member_id=member_id,cf:credit_score=credit_score,cf:ucl=ucl") \
        .load()

    # Join dữ liệu
    lookup_df = member_data.join(member_scores, ["member_id"])

    # Tạo và populate lookup_table trong HBase
    hbase = HBaseDao.get_instance()
    for row in lookup_df.collect():
        row_dict = row.asDict()
        card_id = str(row_dict['card_id']).encode()
        data = {
            b'cf:credit_score': str(row_dict['credit_score']).encode(),
            b'cf:ucl': str(row_dict['ucl']).encode(),
            b'cf:postcode': str(row_dict['postcode']).encode(),
            b'cf:transaction_dt': b''  # Khởi tạo rỗng, cập nhật sau
        }
        hbase.write_data(card_id, data, 'lookup_table')

    spark.stop()

if __name__ == "__main__":
    create_and_populate_lookup()