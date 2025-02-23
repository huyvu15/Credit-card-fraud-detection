import happybase

class HBaseDao:
    __instance = None

    @staticmethod
    def get_instance():
        if HBaseDao.__instance is None:
            HBaseDao()
        return HBaseDao.__instance

    def __init__(self):
        if HBaseDao.__instance is not None:
            raise Exception("This class is a singleton!")
        else:
            HBaseDao.__instance = self
            self.host = 'hbase'  # Tên service trong docker-compose
            self.connect()

    def connect(self):
        for i in range(3):
            try:
                self.pool = happybase.ConnectionPool(size=3, host=self.host, port=9090)
                break
            except:
                print("Exception in connecting HBase")

    def get_data(self, key, table):
        for i in range(2):
            try:
                with self.pool.connection() as connection:
                    t = connection.table(table)
                    row = t.row(key)
                    return row
            except:
                self.reconnect()

    def write_data(self, key, row, table):
        error = None
        for i in range(2):
            try:
                with self.pool.connection() as connection:
                    t = connection.table(table)
                    return t.put(key, row)
            except Exception as e:
                error = e
                self.reconnect()
        # Chuyển key về str trước khi nối với str(error)
        raise Exception(f"{key.decode('utf-8')} - {str(error)}")

    def get_data(self, key, table):
        print(f"Attempting to get data for key: {key}, table: {table}")
        for i in range(2):
            try:
                with self.pool.connection() as connection:
                    t = connection.table(table)
                    row = t.row(key)
                    print(f"Data retrieved: {row}")
                    return row
            except Exception as e:
                print(f"Error getting data: {e}")
                self.reconnect()
    
    def reconnect(self):
        self.connect()