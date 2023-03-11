import os
import configparser

import pymysql


class MysqlInfo:
    def __init__(self, cfg_path=os.path.join(os.getcwd(), 'configure', 'mysql.ini')):
        self.__cfg_path = cfg_path
        os.makedirs(os.path.dirname(cfg_path), exist_ok=True)
        self.__config = configparser.ConfigParser()
        try:
            if not os.path.exists(cfg_path):
                self.__config['MySQLInfo'] = {
                    'host': '',
                    'port': -1,
                    'user': '',
                    'passwd': ''
                }
                with open(cfg_path, 'w') as f:
                    self.__config.write(f)

            self.__config.read(cfg_path)
            self.host = self.__config.get('MySQLInfo', 'host')
            self.port = int(self.__config.get('MySQLInfo', 'port'))
            self.user = self.__config.get('MySQLInfo', 'user')
            self.passwd = self.__config.get('MySQLInfo', 'passwd')
            self.dbname = None
            self.tablename = None
        except (FileNotFoundError, configparser.Error, ValueError) as e:
            print(f"Error while reading configuration file: {e}")

    def update(self, host=None, port=None, user=None, passwd=None):
        try:
            if host is not None:
                self.__config.set('MySQLInfo', 'host', host)
                self.host = host
            if port is not None:
                self.__config.set('MySQLInfo', 'port', str(port))
                self.port = int(port)
            if user is not None:
                self.__config.set('MySQLInfo', 'user', user)
                self.user = user
            if passwd is not None:
                self.__config.set('MySQLInfo', 'passwd', passwd)
                self.passwd = passwd

            with open(self.__cfg_path, 'w') as f:
                self.__config.write(f)
        except (configparser.Error, ValueError):
            return False
        return True

    def read(self):
        try:
            self.__config.read(self.__cfg_path)
            self.host = self.__config.get('MySQLInfo', 'host')
            self.port = int(self.__config.get('MySQLInfo', 'port'))
            self.user = self.__config.get('MySQLInfo', 'user')
            self.passwd = self.__config.get('MySQLInfo', 'passwd')
        except (configparser.Error, ValueError):
            return False
        return True

    def check(self):
        try:
            conn = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.passwd,
                database=self.dbname
            )
            conn.close()
        except pymysql.Error as e:
            print(f"Error connecting to MySQL: {e}")
            return False
        return True


class FTPInfo:
    def __init__(self, cfg_path=os.path.join(os.getcwd(), 'configure', 'ftpinfo.ini')):
        self.__cfg_path = cfg_path
        os.makedirs(os.path.dirname(cfg_path), exist_ok=True)
        self.__config = configparser.ConfigParser()
        try:
            if not os.path.exists(cfg_path):
                self.__config['FTPInfo'] = {
                    'ftp_name': '',
                    'host': '',
                    'port': -1,
                    'user': '',
                    'passwd': '',
                    'sync_path': '',
                    'down_path': '',
                    'filter': ''
                }
                with open(cfg_path, 'w') as f:
                    self.__config.write(f)
            self.__config.read(cfg_path)
            self.ftp_name = self.__config.get('FTPInfo', 'ftp_name')
            self.host = self.__config.get('FTPInfo', 'host')
            self.port = int(self.__config.get('FTPInfo', 'port'))
            self.user = self.__config.get('FTPInfo', 'user')
            self.passwd = self.__config.get('FTPInfo', 'passwd')
            self.sync_path = self.__config.get('FTPInfo', 'sync_path')
            self.down_path = self.__config.get('FTPInfo', 'down_path')
            self.scan_filter = self.__config.get('FTPInfo', 'scan_filter')
        except (FileNotFoundError, configparser.Error, ValueError) as e:
            print(f"Error while reading configuration file: {e}")

    def update(self, ftp_name=None, host=None, port=None, user=None, passwd=None, sync_path=None, down_path=None,
               scan_filter=None):
        if ftp_name is not None:
            self.__config.set('FTPInfo', 'ftp_name', ftp_name)
            self.ftp_name = ftp_name
        if host is not None:
            self.__config.set('FTPInfo', 'host', host)
            self.host = host
        if port is not None:
            self.__config.set('FTPInfo', 'port', str(port))
            self.port = int(port)
        if user is not None:
            self.__config.set('FTPInfo', 'user', user)
            self.user = user
        if passwd is not None:
            self.__config.set('FTPInfo', 'passwd', passwd)
            self.passwd = passwd
        if sync_path is not None:
            self.__config.set('FTPInfo', 'sync_path', sync_path)
            self.sync_path = sync_path
        if down_path is not None:
            self.__config.set('FTPInfo', 'down_path', down_path)
            self.down_path = down_path
        if scan_filter is not None:
            self.__config.set('FTPInfo', 'scan_filter', scan_filter)
            self.scan_filter = scan_filter
        try:
            with open(self.__cfg_path, 'w') as f:
                self.__config.write(f)
        except configparser.Error:
            return False
        return True

    def read(self):
        try:
            self.__config.read(self.__cfg_path)
            self.ftp_name = self.__config.get('FTPInfo', 'ftp_name')
            self.host = self.__config.get('FTPInfo', 'host')
            self.port = int(self.__config.get('FTPInfo', 'port'))
            self.user = self.__config.get('FTPInfo', 'user')
            self.passwd = self.__config.get('FTPInfo', 'passwd')
            self.sync_path = self.__config.get('FTPInfo', 'sync_path')
            self.down_path = self.__config.get('FTPInfo', 'down_path')
            self.scan_filter = self.__config.get('FTPInfo', 'scan_filter')
        except (configparser.Error, ValueError):
            return False
        return True


class ThreadInfo:
    status = False

    def __init__(self, buffer):
        self.buffer = buffer

    def set_status(self, status):
        ThreadInfo.status = status
        self.status = status

