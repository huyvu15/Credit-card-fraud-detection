import pandas as pd
from hbase_dao import HBaseDao
import mysql.connector
from pyspark.sql import SparkSession

def load_csv_to_hbase(csv_path, table_name='card_transactions'):
    hbase = HBaseDao.get_instance()
    df = pd.read_csv(csv_path)
    for _, row in df.iterrows():
        key = str(row['card_id']).encode()
        data = {
            b'cf:member_id': str(row['member_id']).encode(),
            b'cf:amount': str(row['amount']).encode(),
            b'cf:postcode': str(row['postcode']).encode(),
            b'cf:pos_id': str(row['pos_id']).encode(),
            b'cf:transaction_dt': str(row['transaction_dt']).encode(),
            b'cf:status': str(row['status']).encode()
        }
        hbase.write_data(key, data, table_name)
    print(f"Loaded {len(df)} records into {table_name}")

def load_rds_to_hbase():
    conn = mysql.connector.connect(
        host="upgradawsrds1.cyajelc9bmnf.us-east-1.rds.amazonaws.com",
        user="upgraduser",
        password="upgraduser",
        database="cred_financials_data"
    )
    hbase = HBaseDao.get_instance()
    
    # Load card_member
    card_member = pd.read_sql("SELECT * FROM card_member", conn)
    for _, row in card_member.iterrows():
        key = str(row['card_id']).encode()
        data = {b'cf:' + k.encode(): str(v).encode() for k, v in row.items()}
        hbase.write_data(key, data, 'card_member')

    # Load member_score
    member_score = pd.read_sql("SELECT * FROM member_score", conn)
    for _, row in member_score.iterrows():
        key = str(row['member_id']).encode()
        data = {b'cf:score': str(row['score']).encode()}
        hbase.write_data(key, data, 'member_score')
    
    conn.close()
    print("Loaded data from RDS to HBase")

if __name__ == "__main__":
    load_csv_to_hbase('/app/data/card_transactions.csv')
    load_rds_to_hbase()