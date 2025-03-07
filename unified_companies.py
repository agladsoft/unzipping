import re
import abc
import time
import httpx
import sqlite3
import requests
import contextlib
from __init__ import *
from pathlib import Path
from functools import reduce
from pandas import DataFrame
from datetime import datetime
from bs4 import BeautifulSoup
from operator import add, mul
from stdnum.exceptions import *
from dadata.sync import DadataClient
from requests import Response, Session
from stdnum.util import clean, isdigits
import xml.etree.ElementTree as ElemTree
from typing import Union, List, Optional
from deep_translator import GoogleTranslator


logger = get_logger(f"unified_companies {str(datetime.now().date())}")


class UnifiedCompaniesManager:
    def __init__(self):
        self.unified_companies = [
            UnifiedRussianCompanies(),
            UnifiedKazakhstanCompanies(),
            UnifiedBelarusCompanies(),
            UnifiedUzbekistanCompanies()
        ]

    @staticmethod
    def get_valid_company(unified_company, company_data):
        with contextlib.suppress(Exception):
            return unified_company if unified_company.is_valid(company_data) else None

    @staticmethod
    def fetch_company_name(df, company, taxpayer_id):
        rows = company.cur.execute(
            f'SELECT * FROM "{company.table_name}" WHERE taxpayer_id=?',
            (taxpayer_id,)
        ).fetchall()
        return rows[0][1] if rows else company.get_company_by_taxpayer_id(df, taxpayer_id, 3)


class UnifiedContextProcessor:
    @staticmethod
    def unified_values(context: dict, df: DataFrame):
        UnifiedContextProcessor.unify_station(context)
        UnifiedContextProcessor.unify_companies(context, df)

    @staticmethod
    def unify_station(context: dict):
        for value in DICT_STATION['station']:
            if value and value in context['destination_station'].upper():
                index = DICT_STATION['station'].index(value)
                context['destination_station'] = DICT_STATION['station_unified'][index]
                break

    @staticmethod
    def unify_companies(context: dict, df: DataFrame):
        manager = UnifiedCompaniesManager()

        for company in HEADER_LABELS[:4]:
            if company_data := context.get(company):
                taxpayer_id, country, is_found_taxpayer_id = \
                    UnifiedContextProcessor.extract_taxpayer_id(company_data, df)
                context[f"{company}_taxpayer_id"] = taxpayer_id
                context[f"is_found_{company}_taxpayer_id_invoice"] = is_found_taxpayer_id

                if taxpayer_id:
                    for unified_company in manager.unified_companies:
                        if unified_company := manager.get_valid_company(unified_company, taxpayer_id):
                            company_name = manager.fetch_company_name(df, unified_company, taxpayer_id)
                            context[f"{company}_unified"] = company_name

    @staticmethod
    def extract_taxpayer_id(company_data, df: DataFrame):
        valid_company: Optional[object] = None
        # all_digits = re.findall(r"\d+", company_data)
        #
        # for unified_company in manager.unified_companies:
        #     for item_inn in all_digits:
        #         if valid_company := manager.get_valid_company(unified_company, item_inn):
        #             return item_inn

        # If no valid taxpayer ID found, use search engine
        search_engine = SearchEngineParser(valid_company)
        return search_engine.get_company_by_taxpayer_id(df, company_data, 3)


class BaseUnifiedCompanies(abc.ABC):
    def __init__(self):
        self.table_name: str = "cache_taxpayer_id"
        self.conn: sqlite3.Connection = sqlite3.connect(self.create_file_for_cache(), check_same_thread=False)
        self.cur: sqlite3.Cursor = self.load_cache()

    @abc.abstractmethod
    def is_valid(self, number: str) -> bool:
        pass

    @abc.abstractmethod
    def get_company_by_taxpayer_id(self, df: DataFrame, taxpayer_id: str, number_attempts: int) -> Optional[str]:
        pass

    @staticmethod
    def create_file_for_cache() -> str:
        """
        Creating a file for recording INN caches and sentence.
        """
        path_cache: str = f"{os.environ.get('XL_IDP_ROOT_UNZIPPING')}/cache/cache.db"
        fle: Path = Path(path_cache)
        if not os.path.exists(os.path.dirname(fle)):
            os.makedirs(os.path.dirname(fle))
        fle.touch(exist_ok=True)
        return path_cache

    def load_cache(self) -> sqlite3.Cursor:
        """
        Loading the cache.
        """
        cur: sqlite3.Cursor = self.conn.cursor()
        cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.table_name}(
               taxpayer_id TEXT PRIMARY KEY,
               company_name TEXT,
               country TEXT)
            """)
        self.conn.commit()
        return cur

    def get_response(self, url, country, method="GET", data=None, use_proxy=True) -> Optional[Response]:
        """
        Sending a request to the API.
        :param url:
        :param country:
        :param method:
        :param data:
        :param use_proxy:
        :return:
        """
        response: Optional[Response] = None
        proxy: Optional[str] = next(CYCLED_PROXIES) if use_proxy else None
        used_proxy: Optional[str] = None
        try:
            session: Session = requests.Session()
            if use_proxy:
                session.proxies = {"https": proxy}
                used_proxy = proxy
            if method == "POST":
                response = session.post(url, json=data, timeout=120)
            else:
                response = session.get(url, timeout=120)
            logger.info(f"Статус запроса {response.status_code}. URL - {url}. Country - {country}")
            logger.info(f'Использованный прокси: {used_proxy if use_proxy else "Без прокси"}')
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(
                f"Ошибка API-запроса - {e}. Proxy - {used_proxy}. Text - {response.text if response else 'No response'}"
            )
            if use_proxy:
                logger.info("Повторный запрос без прокси...")
                return self.get_response(url, country, method, data, use_proxy=False)
            return None  # Если ошибка без прокси, просто возвращаем None

    def cache_add_and_save(self, taxpayer_id: str, company_name: str, country: str) -> None:
        """
        Saving and adding the result to the cache.
        """
        self.cur.executemany(f"INSERT or REPLACE INTO {self.table_name} VALUES(?, ?, ?)",
                             [(taxpayer_id, company_name, country)])
        self.conn.commit()


class UnifiedRussianCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "russia"

    def __repr__(self):
        return "russia"

    @staticmethod
    def calc_company_check_digit(number):
        """
        Calculate the check digit for the 10-digit ??? for organisations.
        :param number:
        :return:
        """
        weights = (2, 4, 10, 3, 5, 9, 4, 6, 8)
        return str(sum(w * int(n) for w, n in zip(weights, number)) % 11 % 10)

    @staticmethod
    def calc_personal_check_digits(number):
        """"
        "Calculate the check digits for the 12-digit personal ???.
        :param number:
        :return:
        """
        weights = (7, 2, 4, 10, 3, 5, 9, 4, 6, 8)
        d1 = str(sum(w * int(n) for w, n in zip(weights, number)) % 11 % 10)
        weights = (3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8)
        d2 = str(sum(w * int(n) for w, n in zip(weights, number[:10] + d1)) % 11 % 10)
        return d1 + d2

    def validate(self, number):
        """
        Check if the number is a valid ???. This checks the length, formatting and check digit.
        :param number:
        :return:
        """
        number = clean(number, ' ').strip()
        if not isdigits(number):
            raise InvalidFormat()
        if len(number) == 10:
            if self.calc_company_check_digit(number) != number[-1]:
                raise InvalidChecksum()
        elif len(number) == 12:
            # persons
            if self.calc_personal_check_digits(number) != number[-2:]:
                raise InvalidChecksum()
        else:
            raise InvalidLength()
        return number

    def is_valid(self, number):
        """Check if the number is a valid ???."""
        try:
            return bool(self.validate(number))
        except ValidationError:
            return False

    def get_company_by_taxpayer_id(self, df: DataFrame, taxpayer_id: str, number_attempts: int) -> Optional[str]:
        """
        Getting the company name unified from the cache, if there is one.
        Otherwise, we are looking for verification of legal entities on websites.
        :param df:
        :param taxpayer_id:
        :param number_attempts:
        :return:
        """
        dadata: DadataClient = DadataClient(token=DADATA_TOKEN, secret=DADATA_SECRET)
        try:
            dadata_response: list = dadata.find_by_id("party", taxpayer_id)
        except httpx.ConnectError as ex_connect:
            logger.error(f"Failed to connect dadata {ex_connect}. Type error is {type(ex_connect)}. "
                         f"INN is {taxpayer_id}")
            time.sleep(30)
            dadata_response = dadata.find_by_id("party", taxpayer_id)
        except Exception as ex_all:
            logger.error(f"Unknown error in dadata {ex_all}. Type error is {type(ex_all)}. INN is {taxpayer_id}")
            return None
        if dadata_response:
            company_name = dadata_response[0].get('value')
            logger.info(f"Company name is {company_name}. INN is {taxpayer_id}")
            self.cache_add_and_save(taxpayer_id, company_name, self.__str__())
        else:
            company_name = None
        return company_name


class UnifiedKazakhstanCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "kazakhstan"

    def __repr__(self):
        return "kazakhstan"

    @staticmethod
    def multiply(weights: List[int], number: str) -> int:
        return reduce(add, map(lambda i: mul(*i), zip(map(int, number), weights))) # type: ignore

    def is_valid(self, number):
        if len(number) != 12:
            return False
        w1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        w2 = [3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2]
        check_sum = self.multiply(w1, number) % 11
        if check_sum == 10:
            check_sum = self.multiply(w2, number) % 11
        return check_sum == int(number[-1])

    def get_company_by_taxpayer_id(self, df: DataFrame, taxpayer_id: str, number_attempts: int):
        """

        :param df:
        :param taxpayer_id:
        :param number_attempts:
        :return:
        """
        data = {
            "page": "1",
            "size": 10,
            "value": taxpayer_id
        }
        if response := self.get_response("https://pk.uchet.kz/api/web/company/search/", self.__str__(), method="POST",
                                         data=data):
            company_name: Optional[str] = None
            for result in response.json()["results"]:
                company_name = result["name"]
                break
            logger.info(f"Company name is {company_name}. BIN is {taxpayer_id}")
            self.cache_add_and_save(taxpayer_id, company_name, self.__str__())
            return company_name
        return None


class UnifiedBelarusCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "belarus"

    def __repr__(self):
        return "belarus"

    def is_valid(self, number):
        """

        :param number:
        :return:
        """
        if len(number) != 9 or number == '000000000':
            return False

        weights = [29, 23, 19, 17, 13, 7, 5, 3]

        checksum = sum(int(d) * w for d, w in zip(number[:-1], weights))
        checksum = checksum % 11
        if checksum == 10:
            checksum = sum(int(d) * w for d, w in zip(number[:-1], weights[1:]))
            checksum = checksum % 11

        return checksum == int(number[-1])

    def get_company_by_taxpayer_id(self, df: DataFrame, taxpayer_id: str, number_attempts: int):
        """

        :param df:
        :param taxpayer_id:
        :param number_attempts:
        :return:
        """
        if response := self.get_response(f"https://www.portal.nalog.gov.by/grp/getData?unp="
                                         f"{taxpayer_id}&charset=UTF-8&type=json", self.__str__()):
            row = response.json()['row']
            data = {'unp': row['vunp'], 'company_name': row['vnaimk']}
            logger.info(f"Company name is {data['company_name']}. UNP is {taxpayer_id}")
            self.cache_add_and_save(taxpayer_id, data['company_name'], self.__str__())
            return data['company_name']
        return None


class UnifiedUzbekistanCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "uzbekistan"

    def __repr__(self):
        return "uzbekistan"

    def is_valid(self, number):
        return False if len(number) != 9 else bool(re.match(r'[3-8]', number))

    def get_company_by_taxpayer_id(self, df: DataFrame, taxpayer_id: str, number_attempts: int):
        """

        :param df:
        :param taxpayer_id:
        :param number_attempts:
        :return:
        """
        if response := self.get_response(f"http://orginfo.uz/en/search/all?q={taxpayer_id}", self.__str__()):
            soup = BeautifulSoup(response.text, "html.parser")
            a = soup.find_all('div', class_='card-body pt-0')[-1]
            if name := a.find_next('h6', class_='card-title'):
                name = name.text.replace('\n', '').strip()
            logger.info(f"Company name is {name}. INN is {taxpayer_id}")
            try:
                company_name: str = GoogleTranslator(source='uz', target='ru').translate(name[:4500])
            except Exception as e:
                logger.error(f"Exception is {e}")
                company_name = name
            self.cache_add_and_save(taxpayer_id, company_name, self.__str__())
            return company_name
        return None


class SearchEngineParser(BaseUnifiedCompanies):
    def __init__(self, unified_company):
        super().__init__()
        self.table_name = "search_engine"
        self.cur: sqlite3.Cursor = self.load_cache()
        self.unified_company = unified_company

    def is_valid(self, number: str) -> bool:
        pass

    def get_inn_from_site(self, dict_inn: dict, values: list, count_inn: int, unified_companies) -> None:
        """
        Parse each site (description and title).
        """
        for item_inn in values:
            for unified_company in unified_companies:
                with contextlib.suppress(Exception):
                    if self.unified_company and unified_company.is_valid(item_inn):
                        dict_inn[item_inn] = dict_inn[item_inn] + 1 if item_inn in dict_inn else count_inn
                    elif unified_company.is_valid(item_inn):
                        self.unified_company = unified_company
                        dict_inn[item_inn] = dict_inn[item_inn] + 1 if item_inn in dict_inn else count_inn

    @staticmethod
    def get_code_error(error_code: ElemTree, value: str) -> None:
        """
        Getting error codes from xml_river.
        """
        if error_code.tag == 'error':
            code: Union[str, None] = error_code.attrib.get('code')
            message: str = MESSAGE_TEMPLATE.get(code, "Not found code error. Exception - {}. "
                                                      "Value - {}. Error code - {}")
            message = message.format(error_code.text, value, code)
            prefix: str = PREFIX_TEMPLATE.get(code, "необработанная_ошибка_на_строке_")
            logger.error(f"Error code is {code}. Message is {message}. Prefix is {prefix}")
            if code == '200':
                raise AssertionError(message)
            else:
                raise ConnectionRefusedError(message)

    def parse_xml(self, response: Response, value: str, dict_inn: dict, count_inn: int, unified_companies):
        """
        Parsing xml.
        """
        # Parse the XML data
        root = ElemTree.fromstring(response.text)
        self.get_code_error(root[0][0], value)

        # Find all title and passage elements
        for doc in root.findall(".//doc"):
            title = doc.find('title').text or ''
            passage = doc.find('.//passage').text or ''
            inn_text: list = re.findall(r"\d+", passage)
            inn_title: list = re.findall(r"\d+", title)
            self.get_inn_from_site(dict_inn, inn_text, count_inn, unified_companies)
            self.get_inn_from_site(dict_inn, inn_title, count_inn, unified_companies)

    def get_inn_from_search_engine(self, value: str) -> dict:
        """
        Looking for the INN in the search engine, and then we parse through the sites.
        """
        logger.info(f"Before request. Data is {value}")
        try:
            r: Response = requests.get(f"https://xmlriver.com/search_yandex/xml?user={USER_XML_RIVER}"
                                       f"&key={KEY_XML_RIVER}&query={value} ИНН", timeout=120)
        except Exception as e:
            logger.error(f"Run time out. Data is {value}. Exception is {e}")
            raise AssertionError from e
        logger.info(f"After request. Data is {value}")
        dict_inn: dict = {}
        count_inn: int = 1
        unified_companies = [
            UnifiedRussianCompanies(),
            UnifiedKazakhstanCompanies(),
            UnifiedBelarusCompanies(),
            UnifiedUzbekistanCompanies()
        ]
        self.parse_xml(r, value, dict_inn, count_inn, unified_companies)
        logger.info(f"Dictionary with INN is {dict_inn}. Data is {value}")
        return dict_inn

    @staticmethod
    def find_key_in_dataframe(df: DataFrame, search_dict: dict):
        """
        Define the function to search for a string in the DataFrame
        :param df:
        :param search_dict:
        :return:
        """
        def search_string(s, search):
            return search in str(s).lower()

        for key in search_dict:
            # Create a mask by applying the search function to check if the key exists
            mask = df.apply(lambda x: x.map(lambda s: search_string(s, key)))

            # If any value in the mask is True, return the key
            if mask.any().any():
                return key

        # If no key is found, return None
        return None

    def get_company_by_taxpayer_id(self, df: DataFrame, value: str, number_attempts: int):
        """
        Getting the INN from the cache, if there is one. Otherwise, we search in the search engine.
        """
        best_found_inn = None
        is_found_taxpayer_id_invoice = True
        if number_attempts == 0:
            return best_found_inn, self.unified_company
        unwanted_chars = r"[<>\«\»\’\‘\“\”`'\".,!@#$%^&*()\[\]{};?\|~=_+]+"
        value = re.sub(unwanted_chars, "", value)
        value = re.sub(" +", " ", value).strip()
        rows: sqlite3.Cursor = self.cur.execute(f'SELECT * FROM "{self.table_name}" WHERE taxpayer_id=?', (value,), )
        if list_rows := list(rows):
            logger.info(f"Data is {list_rows[0][0]}. INN is {list_rows[0][1]}")
            if not self.find_key_in_dataframe(df, {f"{list_rows[0][1]}": 1}):
                is_found_taxpayer_id_invoice = False
            return list_rows[0][1], list_rows[0][2], is_found_taxpayer_id_invoice
        try:
            api_inn: dict = self.get_inn_from_search_engine(value)
            best_found_inn = self.find_key_in_dataframe(df, api_inn)
            if not best_found_inn:
                is_found_taxpayer_id_invoice = False
                best_found_inn = max(api_inn, key=api_inn.get, default=None)
            self.cache_add_and_save(
                value,
                best_found_inn,
                self.unified_company.__str__() if self.unified_company else self.unified_company
            )
        except ConnectionRefusedError:
            time.sleep(60)
            self.get_company_by_taxpayer_id(df, value, number_attempts - 1)
        return best_found_inn, self.unified_company, is_found_taxpayer_id_invoice


if __name__ == "__main__":
    print(UnifiedRussianCompanies().is_valid("192494228"))
    print(UnifiedKazakhstanCompanies().is_valid("192494228"))
    print(UnifiedBelarusCompanies().is_valid("192494228"))
    print(UnifiedUzbekistanCompanies().is_valid("192494228"))
