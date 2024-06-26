import re
import json
import zipfile
import rarfile
import numpy as np
import pandas as pd
from __init__ import *
from datetime import datetime
from typing import Dict, List, Optional


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, DataExtractor):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


class DataExtractor:
    def __init__(self, filename, directory):
        self.filename = filename
        self.directory = directory
        self.dict_columns_position: Dict[str, Optional[int]] = {
            "model": None,
            "number_pp": None,
            "tnved_code": None,
            "country_of_origin": None,
            "goods_description": None,
            "quantity": None,
            "package_quantity": None,
            "net_weight": None,
            "gross_weight": None,
            "price_per_piece": None,
            "total_cost": None
        }

    @staticmethod
    def _is_digit(x: str) -> bool:
        """
        Checks if a value is a number.
        """
        try:
            float(re.sub(r'(?<=\d) (?=\d)', '', x))
            return True
        except (ValueError, TypeError):
            return False

    @staticmethod
    def _merge_two_dicts(x: Dict, y: Dict) -> dict:
        """
        Merges two dictionaries.
        """
        z: Dict = x.copy()  # start with keys and values of x
        z.update(y)  # modifies z with keys and values of y
        return z

    def _is_table_starting(self, row: list) -> bool:
        """
        Understanding when a headerless table starts.
        """
        return (
                self.dict_columns_position["model"] or
                self._is_digit(row[self.dict_columns_position["number_pp"]])
        ) and row[self.dict_columns_position["tnved_code"]]

    @staticmethod
    def _remove_symbols_in_columns(row: str) -> str:
        """
        Bringing the header column to a unified form.
        """
        row: str = re.sub(r"\n", " ", row).strip() if row else row
        row: str = re.sub(r" +", " ", row).strip() if row else row
        return row

    @staticmethod
    def _get_list_columns() -> List[str]:
        """
        Getting all column names for all lines in the __init__.py file.
        """
        list_columns = []
        for keys in list(DICT_HEADERS_COLUMN_ENG.keys()):
            list_columns.extend(iter(keys))
        return list_columns

    def _get_probability_of_header(self, row: list, list_columns: list) -> int:
        """
        Getting the probability of a row as a header.
        """
        row: list = list(map(self._remove_symbols_in_columns, row))
        count: int = sum(element in list_columns for element in row)
        return int(count / len(row) * 100)

    def write_to_json(self, list_data: list) -> None:
        """
        Write data to json.
        :param list_data:
        :return:
        """
        basename = os.path.basename(self.filename)
        dir_name = os.path.join(self.directory, 'json')
        os.makedirs(dir_name, exist_ok=True)
        output_file_path = os.path.join(dir_name, f'{basename}.json')
        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(list_data, f, ensure_ascii=False, indent=4, cls=JsonEncoder)

    def _get_columns_position(self, rows: list) -> None:
        """
        Get the position of each column in the file to process the row related to that column.
        """
        rows: list = list(map(self._remove_symbols_in_columns, rows))
        for index, column in enumerate(rows):
            for columns in DICT_HEADERS_COLUMN_ENG:
                for column_eng in columns:
                    if column == column_eng:
                        self.dict_columns_position[DICT_HEADERS_COLUMN_ENG[columns]] = index

    def _get_content_before_table(self, rows: list, context: dict) -> Dict[str, str]:
        """
        Getting the date, ship name and voyage in the cells before the table.
        :param rows:
        :param context:
        :return:
        """
        for i, row in enumerate(rows, 1):
            if row:
                column = row.translate({ord(c): "" for c in ":："}).strip()
                for columns in DICT_LABELS:
                    if column in columns:
                        for cell in rows[i:]:
                            if any(cell in key for key in DICT_LABELS.keys()):
                                break
                            if not cell:
                                continue
                            cell = self._remove_symbols_in_columns(cell)
                            context[DICT_LABELS[columns]] = context[DICT_LABELS[columns]] \
                                if context.get(DICT_LABELS[columns]) else cell

    def _get_content_in_table(self, rows: list, list_data: List[dict], context: dict) -> None:
        """
        Getting the data from the table.
        :param rows:
        :param list_data:
        :param context:
        :return:
        """
        parsed_record: dict = {
            'tnved_code': rows[self.dict_columns_position["tnved_code"]].strip()
        }
        parsed_record = self._merge_two_dicts(context, parsed_record)
        list_data.append(parsed_record)

    def main(self):
        """
        Main function.
        :return:
        """
        df = pd.read_excel(self.filename, dtype=str)
        df.dropna(how='all', inplace=True)
        df.replace({np.nan: None, "NaT": None}, inplace=True)
        context: dict = {}
        list_data: List[dict] = []
        for _, rows in df.iterrows():
            rows = list(rows.to_dict().values())
            try:
                if self._get_probability_of_header(rows, self._get_list_columns()) > COEFFICIENT_OF_HEADER_PROBABILITY:
                    self._get_columns_position(rows)
                elif self._is_table_starting(rows):
                    self._get_content_in_table(rows, list_data, context)
            except TypeError:
                self._get_content_before_table(rows, context)
        self.write_to_json(list_data)


class ArchiveExtractor:
    def __init__(self, directory):
        self.logger: logging.getLogger = get_logger(os.path.basename(__file__).replace(".py", "_")
                                                    + str(datetime.now().date()))
        self.root_directory = directory
        self.dir_name = directory
        self.current_dir = None
        self.extension_handlers = {
            '.xlsx': self.read_excel_file,
            '.xls': self.read_excel_file,
            '.zip': self.unzip_archive,
            '.rar': self.unrar_archive
        }

    def read_excel_file(self, file_path):
        """
        Read the Excel file.
        :param file_path:
        :return:
        """
        self.logger.info(f"Найден файл Excel: {file_path}")
        DataExtractor(file_path, self.root_directory).main()
        self.dir_name = self.dir_name.replace(self.current_dir, '')

    def save_archive(self, archive, file_info):
        """
        Save the archive.
        :param archive:
        :param file_info:
        :return:
        """
        if file_info.is_dir():
            return
        extract_to = os.path.dirname(file_info.filename)
        self.current_dir = extract_to
        self.dir_name = os.path.join(self.dir_name, extract_to)
        inner_archive_file = archive.open(file_info.filename)
        inner_archive_filename = os.path.join(self.dir_name, os.path.basename(file_info.filename))
        os.makedirs(self.dir_name, exist_ok=True)
        with open(inner_archive_filename, 'wb') as f:
            f.write(inner_archive_file.read())
        inner_archive_file.close()
        return inner_archive_filename

    def unrar_archive(self, rar_file):
        """
        Unrar the archive.
        :param rar_file:
        :return:
        """
        self.logger.info(f"Найден архив: {rar_file}")
        with rarfile.RarFile(rar_file, 'r') as rar_ref:
            for file_info in rar_ref.infolist():
                if inner_rar_filename := self.save_archive(rar_ref, file_info):
                    self.process_archive(inner_rar_filename)

    def unzip_archive(self, zip_file):
        """
        Unzip the archive.
        :param zip_file:
        :return:
        """
        self.logger.info(f"Найден архив: {zip_file}")
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if inner_zip_filename := self.save_archive(zip_ref, file_info):
                    self.process_archive(inner_zip_filename)

    def process_archive(self, file_path):
        """
        Process the archive.
        :param file_path:
        :return:
        """
        _, ext = os.path.splitext(file_path)
        if handler := self.extension_handlers.get(ext):
            handler(file_path)
        else:
            self.logger.info(f"Найден файл: {file_path}")
            self.dir_name = self.dir_name.replace(self.current_dir, '')

    def main(self):
        """
        Main function.
        :return:
        """
        for root, dirs, files in os.walk(self.dir_name):
            for file in files:
                file_path = os.path.join(root, file)
                self.process_archive(file_path)


if __name__ == '__main__':
    # ArchiveExtractor('/home/timur/sambashare/unzipping').main()
    import sys; DataExtractor(sys.argv[1], sys.argv[2]).main()
