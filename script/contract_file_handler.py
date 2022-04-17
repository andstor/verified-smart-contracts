import re
import json
from json.decoder import JSONDecodeError


class ContractFileHandler():

    def __init__(self, code_format, file_extension):
        self.code_format = code_format
        self.file_extension = file_extension

    def extract(self, data):
        """
        Extracts files from the source code
        """
        extracted_files = []
        do = f"_extract_{self.code_format.lower()}_files"
        if hasattr(self, do) and callable(func := getattr(self, do)):
            files = func(data)
            for item in files:
                if item['source_code'] == '':
                    continue
                extracted_files.append(item)
        else:
            raise Exception(f"No extractor for {self.code_format} format")

        return extracted_files

    def _extract_text_files(self, data):
        files = []
        files_code = re.split(
            '\/\/ File:? (.*' + re.escape(self.file_extension) + ')', data)

        # Single file
        if len(files_code) == 1:
            files.append({'file_path': '', 'source_code': data})
            return files

        # Combined file
        for index, file in enumerate(files_code):
            if index % 2 == 0:
                continue

            file_path = ''
            if len(file) > 1:
                # File name is every other element in files_code array, except for the first element.
                file_path = file
                source_code = files_code[index+1].strip()
            else:
                source_code = file

            files.append({'file_path': file_path, 'source_code': source_code})

        return files

    def _extract_multi_files(self, data):
        files = []
        try:
            record_json = json.loads(data)
        except JSONDecodeError as e:
            print("Can't decode JSON:" + str(e.msg))
            return []

        for file_path, source in record_json.items():
            source_code = source['content']
            files.append({'file_path': file_path, 'source_code': source_code})
        return files

    def _extract_json_files(self, data):
        files = []
        try:
            record_json = json.loads(data)
        except JSONDecodeError as e:
            print("Can't decode JSON:" + str(e.msg))
            return []

        files_code = record_json['sources']
        for file_path, source in files_code.items():
            source_code = source['content']
            files.append({'file_path': file_path, 'source_code': source_code})
        return files
