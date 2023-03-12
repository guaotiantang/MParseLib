import time
import uuid
import ftputil
import threading
import multiprocessing
from TaskLogClass import *
from ftputil.error import FTPOSError


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
    def __init__(self, thread_status, interval=60):
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


class MroTaskScan(threading.Thread):
    """
    任务扫描线程类
    """

    def __init__(self, task, max_process):
        super().__init__()
        self.task = task
        self.max_process = max_process
        self._stop_event = threading.Event()
        self.process_map = {}

    def stop(self):
        """
        停止线程
        """
        self._stop_event.set()
        # 向 MroParse 进程发送停止命令
        for process_id, process in self.process_map.items():
            process.stop_parse()
            process.join()

    def stopped(self):
        """
        判断线程是否停止
        """
        return self._stop_event.is_set()

    def run(self):
        """
        线程执行的任务
        """

        while not self.stopped():
            # 判断MroParse进程数量是否达到最大值
            if len(self.process_map) >= self.max_process:
                time.sleep(1)
                continue
            # 获取一个未开始的任务
            task_list = self.task.task_get_by_status(ftp_name='test', status='unparse', quantity=1)
            if not task_list:
                continue
            task = task_list[0]
            ftp_name, local_path, subpack = task[1], task[2], task[3]
            # 把任务的状态修改为parseing
            # 后续需要添加一个时间字段，当任务超过一小时则判定为执行失败，然后把状态改回unparse,或者判定进程是否存在以作为判定
            self.task.task_set_status(ftp_name, local_path, subpack, 'parseing')
            # 启动MroParse进程
            p = MroParseProc(ftp_name, local_path, subpack)
            p.start()
            self.process_map[p.process_id] = p


class MroParseProc(multiprocessing.Process):
    process_list = []

    def __init__(self, ftp_name, local_path, subpack):
        super().__init__()
        self.ftp_name = ftp_name
        self.local_path = local_path
        self.subpack = subpack
        self.running = True
        self.process_id = str(uuid.uuid4())[:8]
        self.closed_event = multiprocessing.Event()

    def run(self):
        # 向管理器中注册进程
        self.register_parse()
        try:
            while self.running:
                pass
            # 执行处理流程
            self.running = False
        except Exception as e:
            # 当任务失败需要回退任务状态
            print(f"Exception occurred in MroParse: {e}")
        finally:
            self.closed_event.set()
            # 从管理器中注销进程
            self.unregister_parse()

    def stop_parse(self):
        """
        停止进程
        """
        self.running = False
        self.closed_event.wait()

    def register_parse(self):
        MroParseProc.process_list.append(self.process_id)

    def unregister_parse(self):
        MroParseProc.process_list.remove(self.process_id)
