import filetype
import patoolib


def is_supported_archive(filepath):
    # 获取文件的MIME类型
    file_stream = open(filepath, 'rb')
    file_type = filetype.guess_extension(file_stream)
    print(file_type)
    file_stream.close()
    print(patoolib.ArchiveFormats)
    print(file_type in patoolib.ArchiveFormats)


if __name__ == '__main__':
    is_supported_archive('E:\\Users\\NIXEVOL\\Downloads\\迅雷下载\\MRO数据样板\\TD-LTE_MRO_ZTE_OMC1_20230214173000.zip')
