import asyncio
import sys
from MroFtpClass import *
from TaskLogClass import *


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
