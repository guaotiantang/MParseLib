import asyncio
import queue
import sys
import threading

from MroFtpClass import *
from TaskLogClass import *


class DownloadThread(threading.Thread):
    def __init__(self, thread_status, ftp_scan, file_queue, lock, thread_list):
        super().__init__()
        self.status = thread_status
        self.ftp_scan = ftp_scan
        self.file_queue = file_queue
        self.lock = lock
        self.thread_list = thread_list

    def run(self):
        while self.status.status:
            try:
                self.lock.acquire()  # 获取线程锁
                if not self.file_queue.empty():
                    file = self.file_queue.get()
                    self.lock.release()  # 释放线程锁
                    local_file = self.ftp_scan.file_download(file)
                    print(local_file)
                    if local_file is not None:
                        with DownLog() as db:
                            db.savelog(file)
                else:
                    self.lock.release()  # 释放线程锁
                    time.sleep(1)  # 等待一段时间再检查队列是否为空

                if len(self.thread_list) < 2 and not self.file_queue.empty():
                    new_thread = DownloadThread(self.status, self.ftp_scan, self.file_queue, self.lock,
                                                self.thread_list)
                    new_thread.start()
                    self.thread_list.append(new_thread)
            except Exception as e:
                print(f"Error occurred while downloading file: {e}")
            time.sleep(1)


class FtpScanThread(threading.Thread):
    def __init__(self, thread_status, interval):
        super().__init__()
        self.interval = interval
        self.status = thread_status
        self.status.set_status(True)
        self.ftp_scan = FtpScanClass()
        self.file_queue = queue.Queue()
        self.lock = threading.Lock()
        self.thread_list = []
        if self.ftp_scan.ftp is None:
            print('error FTP Connect Fail')
            self.status.set_status(False)
            return

        for i in range(2):
            download_thread = DownloadThread(self.status, self.ftp_scan, self.file_queue, self.lock, self.thread_list)
            self.thread_list.append(download_thread)
            download_thread.start()

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
                    for file in new_files:
                        if not self.status.status:
                            break
                        self.file_queue.put(file)
                    # 等待下载线程完成
                    for download_thread in self.thread_list:
                        download_thread.join()
            except Exception as e:
                print(f"Error occurred while scanning FTP directory: {e}")
            for i in range(self.interval):
                if self.status.status:
                    break
                time.sleep(1)

    def stop(self):
        self.status.set_status(False)
        for download_thread in self.thread_list:
            download_thread.join()


async def handle_user_input():
    global ftp_scan_thread
    status = ThreadInfo('main')
    while True:
        cmd = await asyncio.get_event_loop().run_in_executor(None, input, "Enter command (start, stop): ")
        if cmd == "start":
            if not ftp_scan_thread or not ftp_scan_thread.is_alive():
                ftp_scan_thread = FtpScanThread(status, interval=1)
                ftp_scan_thread.daemon = True
                ftp_scan_thread.start()
                print("Thread started.")
            else:
                print("Thread is started.")
        elif cmd == "stop":
            if ftp_scan_thread and ftp_scan_thread.is_alive():
                ftp_scan_thread.stop()
                ftp_scan_thread.join()
                print("Thread stopped.")
            else:
                print("Thread is not started.")
        elif cmd == "exit":
            if ftp_scan_thread and ftp_scan_thread.is_alive():
                ftp_scan_thread.stop()
                ftp_scan_thread.join()
                print("Thread stopped.")
            sys.exit()
        elif cmd == 'del':
            DownLog().dellog_by_time('2023-03-09 10:00:00')
        else:
            print("Invalid command. Please enter 'start' or 'stop'.")


if __name__ == '__main__':
    mysql = MysqlInfo()
    ftp = FTPInfo()
    mysql.update(host='localhost', port=3306, user='root', passwd='242520')
    ftp.update(ftp_name='test',
               host='172.29.106.147',
               port=21,
               user='nixevol',
               passwd='242520',
               sync_path='/sync/',
               down_path=os.path.join(os.getcwd(), 'sync'),
               scan_filter='tmp|temp')
    mysql.check()
    ftp_scan_thread = None
    DownLog().dellog_by_time()

    asyncio.run(handle_user_input())
