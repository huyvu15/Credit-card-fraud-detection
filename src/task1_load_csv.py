from hbase_dao import HBaseDao
import pandas as pd

def load_csv_to_hbase():
    hbase = HBaseDao.get_instance()
    df = pd.read_csv('/app/data/card_transactions.csv')
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
        hbase.write_data(key, data, 'card_transactions')
    print(f"Loaded {len(df)} records into card_transactions. Expected: 53,292")

if __name__ == "__main__":
    load_csv_to_hbase()