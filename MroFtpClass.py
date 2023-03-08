import os
import sqlite3
import ftputil
from datetime import datetime
from ftputil.error import FTPOSError


class FtpDirDB:
    def __init__(self):
        self.db_file = os.path.join(os.getcwd(), "log", "FtpDir.db")
        if not os.path.exists(os.path.join(os.getcwd(), "log")):
            os.mkdir(os.path.join(os.getcwd(), "log"))
        self.table_name = "MroFileList"
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.conn.execute('PRAGMA journal_mode=WAL')
        self.cursor = self.conn.cursor()
        # 判断数据表是否存在，不存在则创建
        self._create_table()
        self.conn.commit()
        self.closedb = False

    def _create_table(self):
        self.cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{self.table_name}'")
        if not self.cursor.fetchone():
            self.cursor.execute(f"CREATE TABLE {self.table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, "
                                f"ftp_name TEXT NOT NULL, filepath TEXT NOT NULL, "
                                f"log_time DATETIME NOT NULL)")
            self.cursor.execute(f"CREATE UNIQUE INDEX filepath_index ON {self.table_name} (filepath)")
            self.conn.commit()

    def __del__(self):
        try:
            if not self.closedb:
                if self.cursor:
                    self.cursor.close()
                if self.conn:
                    self.conn.close()
                self.closedb = True
        except sqlite3.Error:
            pass

    def isexists(self, ftp_name, filepath):
        with self.conn:
            self.cursor.execute(f"SELECT * FROM {self.table_name} WHERE ftp_name = ? AND filepath = ?",
                                (ftp_name, filepath))
            return bool(self.cursor.fetchone())

    def savelog(self, ftp_name, filepath):
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.conn:
            try:
                self.cursor.execute(f"INSERT INTO {self.table_name} (ftp_name, filepath, log_time) VALUES (?, ?, ?)",
                                    (ftp_name, filepath, now))
            except sqlite3.IntegrityError:
                # 数据库中已存在相同的记录
                return False
            except sqlite3.Error:
                return False
            return True

    def dellog_by_time(self, time=None):
        if time is None:
            time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.conn:
            try:
                self.cursor.execute(f"DELETE FROM {self.table_name} WHERE log_time < ?", (time,))
            except sqlite3.Error as e:
                print("dellog_by_time() error:", e)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        try:
            if not self.closedb:
                if self.cursor:
                    self.cursor.close()
                if self.conn:
                    self.conn.commit()
                    self.conn.close()
                self.closedb = True
        except sqlite3.Error:
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
        ftp_path = self.ftpinfo['syncpath']
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
            print("Error occurred while scanning FTP directory:", e)
            return []
        return new_files

    def save_all_files_log(self):
        """
        将FTP服务器中所有zip文件路径录入数据库
        """
        ftp_name = self.ftpinfo['name']
        ftp_path = self.ftpinfo['syncpath']
        try:
            for root, dirs, files in self.ftp.walk(ftp_path):
                for name in files:
                    ftp_file = self.ftp.path.join(root, name)
                    if not ftp_file.endswith('.zip'):
                        continue
                    if self.db.isexists(ftp_name, ftp_file):
                        continue
                    self.db.savelog(ftp_name, ftp_file)
        except FTPOSError as e:
            print("Error occurred while scanning FTP directory:", e)
            return False
        return True

    def __del__(self):
        self.ftp.close()
