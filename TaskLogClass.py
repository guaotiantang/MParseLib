from ConfigureClass import *
from datetime import datetime
import pymysql


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
            self.cursor.execute(
                f"INSERT INTO {self.mysqlinfo.tablename} (ftp_name, filepath, log_time) VALUES (%s, %s, %s)",
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


class MroTask:
    def __init__(self):
        self.mysqlinfo = MysqlInfo()
        self.mysqlinfo.dbname = 'mroparse'
        self.mysqlinfo.tablename = 'MroTask'
        self._connect()

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

    def __del__(self):
        self.cursor.close()
        self.conn.close()

    def _create_table(self):
        try:
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.mysqlinfo.tablename} (
                    id INT PRIMARY KEY AUTO_INCREMENT,
                    ftp_name VARCHAR(100) NOT NULL,
                    local_path VARCHAR(500) NOT NULL,
                    subpack VARCHAR(100) NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    task_ctime DATETIME DEFAULT CURRENT_TIMESTAMP
                ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """)
            self.cursor.execute(f"CREATE INDEX ftp_name ON {self.mysqlinfo.tablename} (ftp_name)")
            self.cursor.execute(f"CREATE INDEX local_path ON {self.mysqlinfo.tablename} (local_path)")
            self.cursor.execute(f"CREATE INDEX subpack ON {self.mysqlinfo.tablename} (local_path)")
            self.cursor.execute(f"CREATE INDEX status ON {self.mysqlinfo.tablename} (status)")
            self.cursor.execute(f"CREATE INDEX task_ctime ON {self.mysqlinfo.tablename} (task_ctime)")

        except pymysql.Error as e:
            print(f"Error occurred while creating {self.mysqlinfo.tablename} table: {e}")

    def task_add(self, ftp_name, local_path, subpack):
        if not self.cursor:
            self._connect()
        try:
            # 判断是否已存在该任务
            cursor = self.conn.cursor()
            cursor.execute(
                """SELECT COUNT(*) FROM MroTask WHERE ftp_name=%s AND local_path=%s AND subpack=%s""",
                (ftp_name, local_path, subpack))
            count = self.cursor.fetchone()[0]
            if count > 0:
                return True
            else:
                self.cursor.execute(
                    """INSERT INTO MroTask (ftp_name, local_path, subpack, status) VALUES (%s, %s, %s, %s)""",
                    (ftp_name, local_path, subpack, 'unparse'))
                return True
        except pymysql.Error:
            return False

    def task_set_status(self, ftp_name, local_path, subpack, status):
        if not self.cursor:
            self._connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""UPDATE MroTask SET status=%s WHERE ftp_name=%s AND local_path=%s AND subpack=%s""",
                           (status, ftp_name, local_path, subpack))
            return True
        except pymysql.Error:
            return False

    def task_del_sub(self, ftp_name, local_path, subpack):
        if not self.cursor:
            self._connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""DELETE FROM MroTask WHERE ftp_name=%s AND local_path=%s AND subpack=%s""",
                           (ftp_name, local_path, subpack))
            return True
        except pymysql.Error:
            return False

    def task_del_path(self, ftp_name, local_path):
        if not self.cursor:
            self._connect()
        try:
            cursor = self.conn.cursor()
            cursor.execute("""DELETE FROM MroTask WHERE ftp_name=%s AND local_path=%s""",
                           (ftp_name, local_path))
            return True
        except pymysql.Error:
            return False

    def task_get_by_status(self, ftp_name, status='unparse', quantity=1):
        """
        查询任务表中符合条件的任务列表
        :param ftp_name: FTP名称
        :param status: 任务状态，可选，默认为'unparse'
        :param quantity: 查询数量，默认为1条
        :return: 符合条件的任务列表
        """
        try:
            if not self.cursor:
                self._connect()
            with self.conn.cursor() as cursor:
                if quantity is None:
                    limit = f""
                else:
                    limit = f" LIMIT {quantity}"
                if status == 'unparse':
                    sql = f"SELECT * FROM `MroTask` WHERE `ftp_name`=%s AND `status`=%s ORDER BY `task_ctime`()".format(limit)
                    cursor.execute(sql, (ftp_name, status))
                else:
                    sql = f"SELECT * FROM `MroTask` WHERE `ftp_name`=%s ORDER BY `task_ctime`()".format(limit)
                    cursor.execute(sql, (ftp_name,))
                result = cursor.fetchall()
                return result
        except (pymysql.Error, Exception):
            return None


