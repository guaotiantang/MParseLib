from ConfigureClass import *
from datetime import datetime


class DownLog:
    def __init__(self):
        self.mysqlinfo = MysqlInfo()
        self.ftpinfo = FTPInfo()
        self.mysqlinfo.dbname = 'mroparse'
        self.mysqlinfo.tablename = "DownLog"
        self._connect()
        self.closedb = False

    def _connect(self):
        try:
            self.conn = pymysql.connect(
                host=self.mysqlinfo.host,
                port=self.mysqlinfo.port,
                user=self.mysqlinfo.user,
                password=self.mysqlinfo.passwd,
                database=self.mysqlinfo.dbname,
                autocommit=True
            )
            self.cursor = self.conn.cursor()
            self._create_table()
        except pymysql.Error as e:
            print(f"Error connecting to MySQL: {e}")
            self.cursor = None
            self.conn = None

    def _create_table(self):
        # 判断数据表是否存在，不存在则创建
        try:
            self.cursor.execute(
                f"SELECT table_name FROM information_schema.tables WHERE table_name='{self.mysqlinfo.tablename}'")
            if not self.cursor.fetchone():
                self.cursor.execute(f"CREATE TABLE {self.mysqlinfo.tablename} (id INT PRIMARY KEY AUTO_INCREMENT, "
                                    f"ftp_name VARCHAR(255) NOT NULL, filepath VARCHAR(255) NOT NULL, "
                                    f"log_time DATETIME NOT NULL)")
                self.cursor.execute(f"CREATE INDEX filepath_index ON {self.mysqlinfo.tablename} (filepath)")
                self.cursor.execute(f"CREATE INDEX ftp_name_index ON {self.mysqlinfo.tablename} (ftp_name)")
                self.conn.commit()
        except pymysql.Error as e:
            print(f"Error creating table: {e}")

    def isexists(self, filepath):
        if not self.cursor:
            self._connect()

        try:
            self.cursor.execute(f"SELECT * FROM {self.mysqlinfo.tablename} WHERE ftp_name = %s AND filepath = %s",
                                (self.ftpinfo.ftp_name, filepath))
            result = self.cursor.fetchone()
            return bool(result) if result else False
        except pymysql.Error as e:
            print(f"Error checking if record exists: {e}")
            return False

    def savelog(self, filepath):
        if not self.cursor:
            self._connect()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.cursor.execute(f"INSERT INTO {self.mysqlinfo.tablename} (ftp_name, filepath, log_time) VALUES (%s, %s, %s)",
                                (self.ftpinfo.ftp_name, filepath, now))
        except pymysql.IntegrityError:
            # 数据库中已存在相同的记录
            return False
        except pymysql.Error:
            return False
        return True

    def dellog_by_time(self, time=None):
        if not self.cursor:
            self._connect()
        if time is None:
            time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            self.cursor.execute(f"DELETE FROM {self.mysqlinfo.tablename} WHERE log_time < %s", (time,))
        except pymysql.Error:
            return False
        return True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        try:
            if not self.closedb:
                if self.cursor:
                    self.cursor.close()
                if self.conn:
                    if exc_type:
                        self.conn.rollback()
                    else:
                        self.conn.commit()
                    self.conn.close()
                self.closedb = True
        except pymysql.Error:
            pass

    def __del__(self):
        try:
            if not self.closedb:
                if self.cursor:
                    self.cursor.close()
                if self.conn:
                    self.conn.close()
                self.closedb = True
        except pymysql.Error:
            pass
