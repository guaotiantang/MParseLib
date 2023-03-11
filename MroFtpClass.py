import os
import ftputil
from datetime import datetime
from ftputil.error import FTPOSError
import pymysql


class FtpDirDB:
    def __init__(self):
        self.db_host = '127.0.0.1'
        self.db_port = 3306
        self.db_user = 'root'
        self.db_pass = '242520'
        self.db_name = 'mroparse'
        self.table_name = "DownLog"
        self._connect()
        self.closedb = False

    def _connect(self):
        try:
            self.conn = pymysql.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_user,
                password=self.db_pass,
                database=self.db_name,
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
                f"SELECT table_name FROM information_schema.tables WHERE table_name='{self.table_name}'")
            if not self.cursor.fetchone():
                self.cursor.execute(f"CREATE TABLE {self.table_name} (id INT PRIMARY KEY AUTO_INCREMENT, "
                                    f"ftp_name VARCHAR(255) NOT NULL, filepath VARCHAR(255) NOT NULL, "
                                    f"log_time DATETIME NOT NULL)")
                self.cursor.execute(f"CREATE UNIQUE INDEX ftp_name_index ON {self.table_name} (ftp_name)")
                self.cursor.execute(f"CREATE UNIQUE INDEX filepath_index ON {self.table_name} (filepath)")
                self.conn.commit()
        except pymysql.Error as e:
            print(f"Error creating table: {e}")

    def isexists(self, ftp_name, filepath):
        if not self.cursor:
            print('error mysql is closed')
            return False
        with self.conn:
            try:
                self.cursor.execute(f"SELECT * FROM {self.table_name} WHERE ftp_name = %s AND filepath = %s",
                                    (ftp_name, filepath))
                result = self.cursor.fetchone()
                return bool(result) if result else False
            except pymysql.Error as e:
                print(f"Error checking if record exists: {e}")
                return False

    def savelog(self, ftp_name, filepath):
        if not self.cursor:
            print('error mysql is closed')
            return False
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.conn:
            try:
                self.cursor.execute(f"INSERT INTO {self.table_name} (ftp_name, filepath, log_time) VALUES (%s, %s, %s)",
                                    (ftp_name, filepath, now))
            except pymysql.IntegrityError:
                # 数据库中已存在相同的记录
                return False
            except pymysql.Error as e:
                print(f"Error inserting record: {e}")
                return False
            return True

    def dellog_by_time(self, time=None):
        if not self.cursor:
            return False
        if time is None:
            time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.conn:
            try:
                self.cursor.execute(f"DELETE FROM {self.table_name} WHERE log_time < %s", (time,))
            except pymysql.Error as e:
                print(f"Error deleting records: {e}")
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
                        print("ext")
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


class FtpScanClass:
    def __init__(self, ftpinfo):
        self.ftpinfo = ftpinfo
        self.ftp = None
        try:
            self.ftp = ftputil.FTPHost(self.ftpinfo['host'], self.ftpinfo['user'], self.ftpinfo['passwd'])
        except FTPOSError as e:
            raise Exception('Error for Ftp Connect: {}'.format(str(e)))
        self.db = FtpDirDB()

    def scan_newfiles(self):
        """
        扫描FTP目录下所有文件，返回新增的文件列表
        :return: 新增的文件列表
        """
        ftp_name = self.ftpinfo['name']
        ftp_path = self.ftpinfo['sync_path']
        new_files = []
        try:
            for root, dirs, files in self.ftp.walk(ftp_path):
                for name in files:

                    # 判断是否zip文件和是否在数据库中，不在则添加到新文件列表中
                    ftp_file = self.ftp.path.join(root, name)
                    if not ftp_file.endswith('.zip'):
                        continue
                    if self.db.isexists(ftp_name, ftp_file):
                        continue
                    new_files.append(ftp_file)
        except FTPOSError as e:
            print("Error occurred while scanning New FTP directory:", e)
            return []
        return new_files

    def save_all_files_log(self):
        """
        将FTP服务器中所有zip文件路径录入数据库
        """
        ftp_name = self.ftpinfo['name']
        ftp_path = self.ftpinfo['sync_path']
        try:
            for root, dirs, files in self.ftp.walk(ftp_path):
                for name in files:
                    ftp_file = self.ftp.path.join(root, name)
                    if not ftp_file.endswith('.zip'):
                        continue
                    print(self.db.isexists(ftp_name, ftp_file))
                    if self.db.isexists(ftp_name, ftp_file):
                        continue
                    self.db.savelog(ftp_name, ftp_file)
        except FTPOSError as e:
            print("Error occurred while scanning All FTP directory:", e)
            return False
        return True

    def __del__(self):
        self.ftp.close()


class MroParseClass:
    """
    MRO文件处理程序
    """

    def __init__(self, ftp_info, filepath):
        self.ftp_info = ftp_info
        self.ftp = None
        self.filepath = filepath

    def file_download(self):
        """
        下载zip文件并返回本地路径
        """
        try:
            with ftputil.FTPHost(self.ftp_info['host'], self.ftp_info['user'], self.ftp_info['passwd'],
                                 timeout=60) as ftp:
                ftp_path = os.path.dirname(self.filepath)
                ftp.chdir(ftp_path)
                download_path = os.path.join(self.ftp_info['sync_path'], self.ftp_info['name'], self.filepath)
                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                ftp.download(self.filepath, download_path)
                return download_path
        except ftputil.error.FTPOSError as e:
            print(f"Error occurred while connecting to FTP server[{self.ftp_info['name']}]: {e}")
            return ''
        except ftputil.error.FTPIOError as e:
            print(f"Error occurred while downloading file {self.filepath}: {e}")
            return ''

    def file_parse(self, file_path):

        return True
