import threading
import time

from MroFtpClass import FtpScanClass, FtpDirDB


class FtpScanThread(threading.Thread):
    def __init__(self, ftp_info, interval=60):
        super().__init__()
        self.ftpinfo = ftp_info
        self.interval = interval
        self.stopped = False

    def run(self):
        ftp_scan = FtpScanClass(self.ftpinfo)
        print("thread is start")
        while not self.stopped:
            try:
                new_files = ftp_scan.scan_newfiles()
                if new_files:
                    print(f"Found {len(new_files)} new files: {new_files}")
                    for file in new_files:
                        with FtpDirDB() as db:
                            db.savelog(self.ftpinfo['name'], file)
            except Exception as e:
                print(f"Error occurred while scanning FTP directory: {e}")
            for i in range(self.interval):
                if self.stopped:
                    break
                time.sleep(1)

    def stop(self):
        self.stopped = True


if __name__ == '__main__':
    ftpinfo = {
        'name': 'TEST1',
        'host': '172.29.106.147',
        'user': 'nixevol',
        'passwd': '242520',
        'syncpath': '/sync/'
    }

    ftp_scan_thread = None
    while True:
        cmd = input("Enter command (start, stop): ")
        if cmd == "start":
            if not ftp_scan_thread or not ftp_scan_thread.is_alive():
                ftp_scan_thread = FtpScanThread(ftpinfo, interval=10)
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
            exit()
        elif cmd == 'del':
            FtpDirDB().dellog_by_time('2023-03-09 10:00:00')
        else:
            print("Invalid command. Please enter 'start' or 'stop'.")
