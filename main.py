import json
import py7zr
import shutil
import zipfile
import rarfile
from pprint import pprint
from unified_companies import *
from typing import Dict, List, Optional, Callable


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, DataExtractor):
            return obj.__dict__
        return json.JSONEncoder.default(self, obj)


class DataExtractor:
    def __init__(self, filename: str, directory: str, input_data: str):
        self.filename: str = filename
        self.directory: str = directory
        self.input_data: str = input_data
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
        self.logger: logging.getLogger = get_logger(f"data_extractor {str(datetime.now().date())}")

    @staticmethod
    def _is_digit(x: str) -> bool:
        """
        Checks if a value is a number.
        """
        if x is None:
            return False
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
        number_pp_index: Optional[int] = self.dict_columns_position.get("number_pp")

        return (
            self.dict_columns_position["model"] or
            self.dict_columns_position["country_of_origin"] or
            (number_pp_index is not None and self._is_digit(row[number_pp_index])) or
            self.dict_columns_position["goods_description"]
        ) and row[self.dict_columns_position["tnved_code"]] \
            and any(char.isdigit() for char in row[self.dict_columns_position["tnved_code"]])

    def _remove_spaces_and_symbols(self, row: str) -> str:
        """
        Remove spaces.
        """
        row: str = row.translate({ord(c): "" for c in ":："}).strip()
        return self._remove_many_spaces(row)

    @staticmethod
    def _remove_many_spaces(row: str, is_remove_spaces: bool = True) -> str:
        """
        Bringing the header column to a unified form.
        """
        if is_remove_spaces:
            return re.sub(r"\s+", "", row, flags=re.UNICODE).upper() if row else row
        row: str = re.sub(r'[\u4e00-\u9fff]+', "", row) if row else row
        row: str = re.sub(r"\n", " ", row).strip() if row else row
        return re.sub(r"\s+", " ", row).strip() if row else row

    @staticmethod
    def _get_list_columns() -> List[str]:
        """
        Getting all column names for all lines in the __init__.py file.
        """
        list_columns: list = []
        for keys in list(DICT_HEADERS_COLUMN_ENG.values()):
            list_columns.extend(iter(keys))
        return list_columns

    def _get_probability_of_header(self, row: list, list_columns: list) -> Tuple[int, int]:
        """
        Getting the probability of a row as a header.
        """
        row: list = list(filter(lambda x: x is not None, map(self._remove_many_spaces, row)))
        count: int = sum(element in list_columns for element in row)
        probability_of_header: int = int(count / len(row) * 100)
        if probability_of_header != 0 and probability_of_header < COEFFICIENT_OF_HEADER_PROBABILITY:
            self.logger.error(f"Probability of header is {probability_of_header}. Columns is {row}")
        return len(row), probability_of_header

    def get_unique_filename(self, target_dir: str, base_name: str) -> str:
        """
        Checks the existence of the file and adds a suffix (_1, _2, etc.) if the file with the same name exists
        but has a different size. If the file with the same name and size exists, it will be overwritten.
        :param target_dir: Target directory.
        :param base_name: The original file name.
        :return: Unique file name.
        """
        dest_path: str = os.path.join(target_dir, base_name)

        # Check if the file exists and if the size is the same
        if os.path.exists(dest_path):
            original_size: int = os.path.getsize(dest_path)
            current_size: int = os.path.getsize(self.filename)

            if original_size == current_size:
                return dest_path  # Overwrite if the size is the same

            # If sizes differ, find a new filename with a counter suffix
            base, ext = os.path.splitext(base_name)
            counter: int = 1
            while os.path.exists(os.path.join(target_dir, f"{base}_{counter}{ext}")):
                counter += 1
            dest_path = os.path.join(target_dir, f"{base}_{counter}{ext}")

        return dest_path

    def copy_file_to_dir(self, dir_name: str) -> None:
        """
        Copies the file to the specified directory, preventing overwriting, if a file with the same name already exists.
        :param dir_name:
        :return:
        """
        target_dir: str = os.path.join(self.directory, dir_name)
        os.makedirs(target_dir, exist_ok=True)

        base_name: str = os.path.basename(self.filename)
        unique_path: str = self.get_unique_filename(target_dir, base_name)

        shutil.copy(self.filename, unique_path)
        self.logger.info(f"Файл скопирован в {unique_path}")

    def write_to_file(self, list_data: list) -> None:
        """
        Write data to xlsx.
        :return:
        """
        if not list_data:
            self.logger.error(f"В файле не найдены данные для обработки. Файл - {self.filename}")
            self.copy_file_to_dir("errors_excel")
        else:
            self.write_to_json(list_data)
            self.copy_file_to_dir("done_excel")

    def write_to_json(self, list_data: list) -> None:
        """
        Write data to json.
        :param list_data:
        :return:
        """
        self.logger.info(f"Данные записываются в файл json. Файл - {self.filename}")

        dir_name: str = os.path.join(self.directory, 'json')
        os.makedirs(dir_name, exist_ok=True)

        base_name: str = f"{os.path.basename(self.filename)}.json"
        output_file_path: str = self.get_unique_filename(dir_name, base_name)

        with open(output_file_path, 'w', encoding='utf-8') as f:
            json.dump(list_data, f, ensure_ascii=False, indent=4, cls=JsonEncoder)

        self.logger.info(f"Файл сохранён как {output_file_path}")

    def _get_columns_position(self, rows: list) -> None:
        """
        Get the position of each column in the file to process the row related to that column.
        """
        rows: list = list(map(self._remove_many_spaces, rows))
        for index, column in enumerate(rows):
            for uni_columns, columns in DICT_HEADERS_COLUMN_ENG.items():
                for column_eng in columns:
                    if column == column_eng:
                        self.dict_columns_position[uni_columns] = index
        self.logger.info(f"Columns position is {self.dict_columns_position}")

    def is_all_right_columns(self, context: dict) -> bool:
        if (
            (context.get(HEADER_LABELS[0]) or context.get(HEADER_LABELS[1]))
            and (context.get(HEADER_LABELS[2]) or context.get(HEADER_LABELS[3]))
            and context.get(HEADER_LABELS[4])
        ):
            return True
        pprint(context)
        print(self.filename)
        self.logger.error(f"В файле нету нужных полей. Файл - {self.filename}")
        return False

    def _get_address_same_keys(self, context: dict, i: int, count_address: int, row: str, rows: list) -> int:
        """
        Getting an address with the same keys.
        :param context:
        :param i:
        :param count_address:
        :param row:
        :param rows:
        :return:
        """
        if row.strip() in DESTINATION_STATION_LABELS and not context.get(row):
            count_address += 1
            if count_address == 2:
                for cell in rows[i:]:
                    if not cell:
                        continue
                    cell: str = self._remove_many_spaces(cell, is_remove_spaces=False)
                    context["destination_station"] = context[row] if context.get(row) else cell
        return count_address

    def merge_sells(self, count_address: int, context: dict, i: int, row: str, rows: list) -> None:
        """
        Merging sells.
        :param count_address:
        :param context:
        :param i:
        :param row:
        :param rows:
        :return:
        """
        if row.strip() in DESTINATION_STATION_LABELS:
            columns: list = list(DICT_LABELS.keys())
            col_map: dict = {1: 0, 3: 1, 4: 3}  # Сопоставление count_address с индексом колонки

            for cell in rows[i:]:
                if cell and cell.strip():
                    cell: str = self._remove_many_spaces(cell, is_remove_spaces=False)
                    if count_address in col_map and context.get(columns[col_map[count_address]]):
                        context[columns[col_map[count_address]]] += f" {cell}"
                    break

    def _get_content_before_table(self, rows: list, context: dict, count_address: int) -> int:
        """
        Getting the date, ship name, and voyage in the cells before the table.
        :param rows:
        :param context:
        :param count_address:
        :return:
        """
        range_index: dict = {}

        for i, row in enumerate(rows, start=1):
            if i == len(rows) and range_index:
                range_index[next(reversed(range_index))].append(i)
            if not row:
                continue

            count_address: int = self._get_address_same_keys(context, i, count_address, row, rows)
            self.merge_sells(count_address, context, i, row, rows)
            self._process_row(row, i, context, range_index)

        self._extract_values_from_range(rows, range_index, context)
        return count_address

    def _process_row(self, row: str, index: int, context: dict, range_index: dict) -> None:
        """
        Processes a row and updates context and range_index accordingly.
        :param row:
        :param index:
        :param context:
        :param range_index:
        :return:
        """
        splitter_column: list = re.split(r'[:：]', row)
        key_cleaned: str = self._remove_spaces_and_symbols(row)

        for uni_columns, columns in DICT_LABELS.items():
            if key_cleaned in columns:
                if range_index:
                    range_index[next(reversed(range_index))].append(index)
                range_index.setdefault(uni_columns, []).append(index)
            elif self._remove_spaces_and_symbols(splitter_column[0]) in columns:
                context[uni_columns] = " ".join(splitter_column[1:]).strip() or splitter_column[-1].strip()

    def _extract_values_from_range(self, rows: list, range_index: dict, context: dict) -> None:
        """
        Extracts the most relevant content from the given range of indices.
        :param rows:
        :param range_index:
        :param context:
        :return:
        """
        for key, value in range_index.items():
            cells: list = rows[value[0]:value[-1] - 1]
            cell: Optional[str] = next((x for x in cells if x is not None), None) \
                if key == "destination_station" else max(filter(None, cells), key=len, default=None)

            if not cell or not cell.strip() or self._is_digit(cell):
                continue

            context.setdefault(key, self._remove_many_spaces(cell, is_remove_spaces=False))

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

    def add_basic_columns(self) -> dict:
        """
        Adding basic columns.
        :return:
        """
        context: dict = {
            "original_file_name": os.path.basename(self.filename),
            "original_file_parsed_on": str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            "input_data": self.input_data
        }
        if container_number := re.findall(r"[A-Z]{4}\d{7}", os.path.basename(self.filename)):
            context[HEADER_LABELS[5]] = container_number[0]
        return context

    def parse_rows(self, df: pd.DataFrame, list_data: List[dict]) -> Optional[List[dict]]:
        """
        Parse rows.
        :param df:
        :param list_data:
        :return:
        """
        context: dict = self.add_basic_columns()
        count_address: int = 0
        for _, rows in df.iterrows():
            rows = list(rows.to_dict().values())
            try:
                len_rows, coefficient = self._get_probability_of_header(rows, self._get_list_columns())
                if coefficient >= COEFFICIENT_OF_HEADER_PROBABILITY and len_rows >= LEN_COLUMNS_IN_ROW:
                    if not self.is_all_right_columns(context):
                        return
                    UnifiedContextProcessor.unified_values(context, df)
                    self._get_columns_position(rows)
                elif self._is_table_starting(rows):
                    self._get_content_in_table(rows, list_data, context)
                else:
                    count_address = self._get_content_before_table(rows, context, count_address)
            except Exception as ex:
                self.logger.error(
                    f"Ошибка при обработке файла {self.filename}: {ex}. Предположительно нету ключа tnved_code"
                )
                self.copy_file_to_dir("errors_excel")
                break
        return list_data

    def read_excel_file(self) -> None:
        """
        Read the Excel file.
        :return:
        """
        list_data: List[dict] = []
        try:
            sheets = pd.ExcelFile(self.filename).sheet_names
            self.logger.info(f"Sheets is {sheets}")
            for sheet in sheets:
                df = pd.read_excel(self.filename, sheet_name=sheet, dtype=str)
                df = df.dropna(how='all').replace({np.nan: None, "NaT": None})
                self.parse_rows(df, list_data)
                if list_data:
                    break
        except Exception as ex:
            self.logger.error(f"Ошибка при чтении файла {self.filename}: {ex}")
            self.copy_file_to_dir("errors_excel")
        self.write_to_file(list_data)


class ArchiveExtractor:
    def __init__(self, directory: str):
        self.logger: logging.getLogger = get_logger(f"archive_extractor {str(datetime.now().date())}")
        self.input_data: Optional[str] = None
        self.root_directory: str = directory
        self.dir_name: str = os.path.join(directory, 'archives')
        self.clear_directory()
        self.extension_handlers: dict = {
            '.xlsx': self.read_excel_file,
            '.xls': self.read_excel_file,
            '.zip': self.unzip_archive,
            '.rar': self.unrar_archive,
            '.7z': self.seven_zip_archive,
            '': self.into_dirs
        }

    def read_excel_file(self, file_path: str) -> None:
        """
        Read the Excel file.
        :param file_path:
        :return:
        """
        self.logger.info(f"Найден файл Excel: {file_path}")
        DataExtractor(file_path, self.root_directory, self.input_data).read_excel_file()

    def clear_directory(self) -> None:
        """
        Clear the directory.
        :return:
        """
        with contextlib.suppress(FileNotFoundError):
            shutil.rmtree(self.dir_name)
        os.makedirs(self.dir_name, exist_ok=True)

    def save_archive(
        self,
        archive: Union[rarfile.RarFile, zipfile.ZipFile],
        file_info: Union[rarfile.RarInfo, zipfile.ZipInfo]
    ) -> Optional[str]:
        """
        Save the archive.
        :param archive:
        :param file_info:
        :return:
        """
        if file_info.is_dir():
            return
        extract_to = os.path.dirname(file_info.filename)
        inner_archive_filename = os.path.join(self.dir_name, extract_to, os.path.basename(file_info.filename))
        os.makedirs(os.path.dirname(inner_archive_filename), exist_ok=True)
        try:
            inner_archive_file = archive.open(file_info.filename)
            with open(inner_archive_filename, 'wb') as f:
                f.write(inner_archive_file.read())
            inner_archive_file.close()
            return inner_archive_filename
        except Exception as ex:
            self.logger.error(f"Ошибка при извлечении файла {file_info.filename}: {ex}")

    def into_dirs(self, dir_name: str) -> None:
        """
        Entry to dir.
        :param dir_name:
        :return:
        """
        for item in os.listdir(dir_name):
            item_path = os.path.join(dir_name, item)
            self.process_archive(item_path)

    def seven_zip_archive(self, seven_zip_file: str) -> None:
        """
        Extract and process a 7z archive.
        :param seven_zip_file: Path to the 7z archive file.
        :return:
        """
        self.logger.info(f"Найден архив: {seven_zip_file}")
        file_list: list = py7zr.SevenZipFile(seven_zip_file, 'r').getnames()  # Получаем список файлов
        for file_name in file_list:
            extract_path: str = os.path.join(self.dir_name, os.path.dirname(file_name))
            os.makedirs(extract_path, exist_ok=True)
            try:
                with py7zr.SevenZipFile(seven_zip_file, 'r') as seven_zip_ref:
                    seven_zip_ref.extract(targets=[file_name], path=extract_path)  # Извлекаем файл
                extracted_file_path: str = os.path.join(extract_path, file_name)
                self.process_archive(extracted_file_path)
            except Exception as ex:
                self.logger.error(f"Ошибка при извлечении файла {file_name}: {ex}")

    def unrar_archive(self, rar_file: str) -> None:
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

    def unzip_archive(self, zip_file: str) -> None:
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

    def process_archive(self, file_path: str) -> None:
        """
        Process the archive.
        :param file_path:
        :return:
        """
        if os.path.isdir(file_path):
            _, ext = file_path, ''
        else:
            _, ext = os.path.splitext(file_path)
        ext: str = ext.lower()
        handler: Callable[[str], None]
        if handler := self.extension_handlers.get(ext):
            try:
                handler(file_path)
            except Exception as ex:
                logger.error(f"Exception is {ex}.")
                errors: str = os.path.join(self.root_directory, "errors")
                os.makedirs(errors, exist_ok=True)
                os.rename(file_path, os.path.join(errors, os.path.basename(file_path)))
        else:
            self.logger.info(f"Найден файл: {file_path}")

    @staticmethod
    def is_file_fully_loaded(file_path: str, wait_time: int = 10) -> bool:
        """
        Check if a file is fully loaded by comparing its size over time.
        :param file_path: Path to the file to check
        :param wait_time: Time to wait between size checks (in seconds)
        :return: True if the file is fully loaded, False otherwise
        """
        initial_size = os.path.getsize(file_path)
        time.sleep(wait_time)
        final_size = os.path.getsize(file_path)
        return initial_size == final_size

    def main(self) -> None:
        """
        Main function.
        :return:
        """
        for _, dirs, files in os.walk(self.root_directory):
            for file in files + list(set(dirs) - set(BASE_DIRECTORIES)):
                file_path: str = os.path.join(self.root_directory, file)

                # Check if the file is fully loaded
                if self.is_file_fully_loaded(file_path, wait_time=WAITING_TIME):
                    self.input_data = file
                    self.process_archive(file_path)
                    done: str = os.path.join(self.root_directory, "done")
                    os.makedirs(done, exist_ok=True)
                    os.rename(file_path, os.path.join(done, file))
            break


if __name__ == '__main__':
    ArchiveExtractor(os.environ["XL_IDP_PATH_UNZIPPING"]).main()
