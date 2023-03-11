import time

import ftputil
from ftputil.error import FTPOSError
from TaskLogClass import *
from ConfigureClass import *


class FtpScanClass:
    def __init__(self):
        self.ftpinfo = FTPInfo()
        self.ftpinfo.read()
        self.ftp = None
        self.mainstatus = ThreadInfo('main')
        try:

            self.ftp = ftputil.FTPHost(self.ftpinfo.host,
                                       self.ftpinfo.user,
                                       self.ftpinfo.passwd,
                                       self.ftpinfo.port,
                                       timeout=60)
        except (FTPOSError, Exception) as e:
            print('Error for Ftp Connect: {}'.format(str(e)))
        self.db = DownLog()

    def scan_newfiles(self):
        """
        扫描FTP目录下所有文件，返回新增的文件列表
        :return: 新增的文件列表
        """
        self.ftpinfo.read()
        ftp_path = self.ftpinfo.sync_path
        scan_filter = self.ftpinfo.scan_filter.split('|')
        new_files = []
        try:
            for root, dirs, files in self.ftp.walk(ftp_path):
                for name in files:
                    # 判断是否zip文件和是否在数据库中，不在则添加到新文件列表中
                    ftp_file = self.ftp.path.join(root, name)
                    if ftp_file.endswith('.zip') and not self.db.isexists(ftp_file):
                        dir_name = self.ftp.path.dirname(ftp_path)
                        if not any(temp_dir in dir_name for temp_dir in scan_filter):
                            new_files.append(ftp_file)

        except (FTPOSError, Exception) as e:
            print("Error occurred while scanning New FTP directory:", e)
            return []

        # 按照文件大小排序
        sorted_files = sorted(new_files, key=lambda f: self.ftp.stat(f).st_size)
        return sorted_files

    def save_all_files_log(self):
        """
        将FTP服务器中所有zip文件路径录入数据库
        """
        self.ftpinfo.read()
        ftp_path = self.ftpinfo.sync_path
        try:
            for root, dirs, files in self.ftp.walk(ftp_path):
                for name in files:
                    ftp_file = self.ftp.path.join(root, name)
                    if not ftp_file.endswith('.zip'):
                        continue
                    print(self.db.isexists(ftp_file))
                    if self.db.isexists(ftp_file):
                        continue
                    self.db.savelog(ftp_file)
        except (FTPOSError, Exception) as e:
            print("Error occurred while scanning All FTP directory:", e)
            return False
        return True

    def file_download(self, filepath):
        """
        下载zip文件并返回本地路径
        """
        try:
            self.ftpinfo.read()
            with ftputil.FTPHost(self.ftpinfo.host, self.ftpinfo.user, self.ftpinfo.passwd, self.ftpinfo.port,
                                 timeout=60) as ftp:
                ftp_path = os.path.dirname(filepath)
                ftp.chdir(ftp_path)
                download_path = os.path.join(
                    self.ftpinfo.down_path,
                    self.ftpinfo.ftp_name,
                    *os.path.normpath(filepath).split(os.path.sep))

                os.makedirs(os.path.dirname(download_path), exist_ok=True)
                # 获取文件初始大小
                init_size = ftp.stat(filepath).st_size
                # 记录文件大小变化时间戳
                size_change_time = time.time()
                # 检测文件大小是否发生变化，如果在一段时间内文件大小未发生变化则开始下载
                while True:
                    if not self.mainstatus.status:
                        break
                    time.sleep(3)  # 暂停3秒
                    cur_size = ftp.stat(filepath).st_size
                    if cur_size != init_size:
                        init_size = cur_size
                        size_change_time = time.time()
                    elif time.time() - size_change_time >= 9:
                        # 文件大小未发生变化9秒，判定为对方已经上传完成
                        ftp.download(filepath, download_path)
                        return download_path

        except (ftputil.error.FTPIOError, Exception) as e:
            print(f"Error occurred while downloading file {filepath}: {e}")
            return None

    def __del__(self):
        if self.ftp is not None:
            self.ftp.close()
