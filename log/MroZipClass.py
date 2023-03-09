import io
import filetype
import zipfile
import patoolib


class MroParse:
    """
    MRO文件处理程序
    """

    def __init__(self, file_stream):
        # self.file_stream = io.BytesIO(open(filepath, 'rb').read())
        self.file_stream = file_stream

    def get_type(self):
        """
        通过filetype库判断文件类型，返回文件类型
        """

        self.file_stream.seek(0)
        file_type = filetype.guess_mime(self.file_stream)
        if file_type == 'application/zip':
            return file_type
        else:
            self.file_stream.seek(0)
            file_type = filetype.guess_extension(self.file_stream)
            if file_type not in patoolib.ArchiveFormats:
                raise ValueError(f"Unsupported file type: {file_type}")
        return file_type

    def get_sub_list(self):
        """
        获取压缩包内的文件列表
        """
        file_type = self.get_type()
        self.file_stream.seek(0)
        if file_type == 'application/zip':
            try:
                with zipfile.ZipFile(self.file_stream, 'r') as zip_file:
                    file_list = zip_file.namelist()
            except zipfile.BadZipFile:
                file_list = []
        else:
            try:
                with patoolib.extract_archive(self.file_stream, verbosity=-1) as archive:
                    file_list = [member.filename for member in archive.get_members()]
            except patoolib.util.PatoolError:
                file_list = []
        self.file_stream.seek(0)
        return file_list

    def get_sub_io(self, filename):
        """
        获取压缩包内指定文件的数据流对象
        """
        file_type = self.get_type()
        self.file_stream.seek(0)
        if file_type == 'application/zip':
            try:
                with zipfile.ZipFile(self.file_stream, 'r') as zip_file:
                    file_bytes = zip_file.read(filename)
            except zipfile.BadZipFile:
                return None
        else:
            try:
                with patoolib.extract_archive(self.file_stream, verbosity=-1) as archive:
                    file_bytes = archive.get_member(filename).read()
            except patoolib.util.PatoolError:
                return None
        self.file_stream.seek(0)
        return io.BytesIO(file_bytes)

    def get_xml_list(self, subzipio):
        """
        通过filetype库判断io数据流对应的文件是zip压缩包还是7z或者tar或者其他类型，
        并且返回该压缩包内所有的xml文件列表
        """
        try:
            with MroParse(subzipio) as parser:
                file_list = parser.get_sub_list()
                xml_list = [f for f in file_list if f.endswith('.xml')]
                return xml_list
        except Exception:
            return []

    def get_xml_io(self, xmlfile, subzipio):
        """
        通过filetype库判断io数据流对应的文件是zip压缩包还是7z或者tar或者其他类型，
        并且返回该压缩包内指定的xml文件的io数据流对象
        """
        try:
            with MroParse(subzipio) as parser:
                file_io = parser.get_sub_io(xmlfile)
                return file_io
        except Exception:
            return None
