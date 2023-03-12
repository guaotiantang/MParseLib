import os
import time

import filetype
import patoolib
import ConfigureClass
import psutil




def is_supported_archive(filepath):
    # 获取文件的MIME类型
    file_stream = open(filepath, 'rb')
    file_type = filetype.guess_extension(file_stream)
    print(file_type)
    file_stream.close()
    print(patoolib.ArchiveFormats)
    print(file_type in patoolib.ArchiveFormats)


if __name__ == '__main__':
    # is_supported_archive('E:\\Users\\NIXEVOL\\Downloads\\迅雷下载\\MRO数据样板\\TD-LTE_MRO_ZTE_OMC1_20230214173000.zip')
    # 获取 CPU 核心数
    while True:
        print(psutil.cpu_percent(interval=1, percpu=True))
