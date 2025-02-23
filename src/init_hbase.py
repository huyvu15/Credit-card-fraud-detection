import happybase
import time

def init_hbase_tables():
    max_retries = 5
    retry_delay = 5  # seconds
    for attempt in range(max_retries):
        try:
            connection = happybase.Connection('hbase', port=9090)
            print("Connected to HBase successfully")
            
            # Danh sách bảng cần tạo
            tables = ['card_transactions', 'lookup_table']
            column_families = {'cf': dict()}  # Column family 'cf'

            # Kiểm tra và tạo bảng
            existing_tables = connection.tables()
            print(f"Existing tables: {existing_tables}")
            for table in tables:
                if table.encode() not in existing_tables:
                    print(f"Creating table {table}")
                    connection.create_table(table, column_families)
                else:
                    print(f"Table {table} already exists")
            
            connection.close()
            print("HBase tables initialized successfully")
            break  # Thoát vòng lặp nếu thành công
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise Exception("Failed to initialize HBase tables after retries")

if __name__ == "__main__":
    init_hbase_tables()