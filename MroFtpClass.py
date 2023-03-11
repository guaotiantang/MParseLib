import threading
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
        扫描FTP目录下所有文件，返回新增的文件列表及相关信息
        :return: 新增的文件列表及相关信息
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
                            file_size = self.ftp.path.getsize(ftp_file)
                            file_mtime = time.time()  # 记录当前扫描时间
                            file_info = (ftp_file, file_size, file_mtime)
                            new_files.append(file_info)

        except (FTPOSError, Exception) as e:
            print("Error occurred while scanning New FTP directory:", e)
            return []

        # 按照文件大小排序
        sorted_files = sorted(new_files, key=lambda f: f[1])
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

    def file_download(self, file_info):
        """
        下载zip文件并返回本地路径
        """
        filepath = file_info[0]
        try:
            self.ftpinfo.read()
            filesize = file_info[1]
            scantime = file_info[2]

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
                init_size = filesize
                # 记录文件大小变化时间戳
                size_change_time = scantime
                # 检测文件大小是否发生变化，如果在一段时间内文件大小未发生变化则开始下载
                while True:
                    if not self.mainstatus.status:
                        break
                    cur_size = ftp.stat(filepath).st_size
                    if cur_size != init_size:
                        init_size = cur_size
                        size_change_time = time.time()
                    elif time.time() - size_change_time >= 9:
                        # 文件大小未发生变化9秒，判定为对方已经上传完成
                        ftp.download(filepath, download_path)
                        return download_path
                    time.sleep(3)  # 暂停3秒

        except (ftputil.error.FTPIOError, Exception) as e:
            print(f"Error occurred while downloading file {filepath}: {e}")
            return None

    def __del__(self):
        if self.ftp is not None:
            self.ftp.close()


class FtpScanThread(threading.Thread):
    def __init__(self, thread_status, interval):
        super().__init__()
        self.interval = interval
        self.status = thread_status
        self.status.set_status(True)

    def run(self):
        ftp_scan = FtpScanClass()
        if ftp_scan.ftp is None:
            print('error FTP Connect Fail')
            self.status.set_status(False)

        while self.status.status:
            try:
                new_files = ftp_scan.scan_newfiles()
                if new_files:
                    print(f"Found {len(new_files)} new files")
                    for file_info in new_files:
                        if not self.status.status:
                            break
                        local_file = ftp_scan.file_download(file_info)
                        if local_file is not None:
                            with DownLog() as db:
                                db.savelog(file_info[0])

            except Exception as e:
                print(f"Error occurred while scanning FTP directory: {e}")
            for i in range(self.interval):
                if self.status.status:
                    break
                time.sleep(1)

    def stop(self):
        self.status.set_status(False)
