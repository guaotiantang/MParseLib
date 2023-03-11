import os
import time

import filetype
import patoolib
import ConfigureClass


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
    path = '/path/to/temp/dir/file.txt'
    temp_dirs = ['tmp', 'temp']
    dir_name = os.path.dirname(path)
    print(dir_name)
    if any(temp_dir in dir_name for temp_dir in temp_dirs):
        print("The directory name contains 'tmp' or 'temp'")
    else:
        print("The directory name does not contain 'tmp' or 'temp'")