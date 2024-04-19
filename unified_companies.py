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
from datetime import datetime
from bs4 import BeautifulSoup
from requests import Response
from operator import add, mul
from stdnum.exceptions import *
from dadata.sync import DadataClient
from stdnum.util import clean, isdigits
import xml.etree.ElementTree as ElemTree
from deep_translator import GoogleTranslator
from typing import Union, Tuple, List, Optional


class BaseUnifiedCompanies(abc.ABC):
    def __init__(self):
        self.table_name: str = "cache_taxpayer_id"
        self.conn: sqlite3.Connection = sqlite3.connect(self.create_file_for_cache(), check_same_thread=False)
        self.cur: sqlite3.Cursor = self.load_cache()

    @abc.abstractmethod
    def is_valid(self, number: str) -> bool:
        pass

    @abc.abstractmethod
    def get_company_by_taxpayer_id(self, taxpayer_id: str) -> Optional[str]:
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

    def get_company_by_taxpayer_id(self, taxpayer_id: str) -> Optional[str]:
        """
        Getting the company name unified from the cache, if there is one.
        Otherwise, we are looking for verification of legal entities on websites.
        :param taxpayer_id:
        :return:
        """
        logger: logging.getLogger = get_logger(f"unified_russian_companies {str(datetime.now().date())}")

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
        return reduce(add, map(lambda i: mul(*i), zip(map(int, number), weights)))

    def is_valid(self, number):
        if not re.match(r'[0-9]{12}', number):
            return False
        w1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        w2 = [3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2]
        check_sum = self.multiply(w1, number) % 11
        if check_sum == 10:
            check_sum = self.multiply(w2, number) % 11
        return check_sum == int(number[-1])

    def get_company_by_taxpayer_id(self, taxpayer_id: str):
        """
        
        :param taxpayer_id:
        :return:
        """
        logger: logging.getLogger = get_logger(f"unified_kazakhstan_companies {str(datetime.now().date())}")
        url = 'https://statsnet.co/search/kz/'
        answer = requests.get(f"{url}/{taxpayer_id}")
        if answer.status_code != 200:
            return None
        soup = BeautifulSoup(answer.text)
        a = soup.find_all('a', class_='text-lg sm:text-xl flex items-center gap-1 text-statsnet hover:text-orange '
                                      'font-stem')[0]
        name = a.find_next('h2').text.replace('\n', '').strip()
        logger.info(f"Company name is {name}. IIN is {taxpayer_id}")
        try:
            company_name: str = GoogleTranslator(source='en', target='ru').translate(name[:4500])
        except Exception as e:
            logger.error(f"Exception is {e}")
            company_name = name
        self.cache_add_and_save(taxpayer_id, company_name, self.__str__())
        return company_name


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
        if len(number) != 9:
            return False

        weights = [29, 23, 19, 17, 13, 7, 5, 3]

        checksum = sum(int(d) * w for d, w in zip(number[:-1], weights))
        checksum = checksum % 11
        if checksum == 10:
            checksum = sum(int(d) * w for d, w in zip(number[:-1], weights[1:]))
            checksum = checksum % 11

        return checksum == int(number[-1])

    def get_company_by_taxpayer_id(self, taxpayer_id: str):
        """
        
        :param taxpayer_id:
        :return:
        """
        logger: logging.getLogger = get_logger(f"unified_belarus_companies {str(datetime.now().date())}")
        answer = requests.get(f"https://www.portal.nalog.gov.by/grp/getData?unp={taxpayer_id}&charset=UTF-8&type=json")
        if answer.status_code != 200:
            logger.error(f"Status code is {answer.status_code}")
            return None
        answer = answer.json()['row']
        data = {'unp': answer['vunp'], 'company_name': answer['vnaimk']}
        logger.info(f"Company name is {data['company_name']}. UNP is {taxpayer_id}")
        self.cache_add_and_save(taxpayer_id, data['company_name'], self.__str__())
        return data['company_name']


class UnifiedUzbekistanCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "uzbekistan"

    def __repr__(self):
        return "uzbekistan"

    def is_valid(self, number):
        return False if len(number) != 9 else bool(re.match(r'[3-8]', number))

    def get_company_by_taxpayer_id(self, taxpayer_id: str):
        """

        :param taxpayer_id:
        :return:
        """
        logger: logging.getLogger = get_logger(f"unified_uzbekistan_companies {str(datetime.now().date())}")
        answer = requests.get(f"http://orginfo.uz/en/search/all?q={taxpayer_id}", timeout=180)
        if answer.status_code != 200:
            return None
        soup = BeautifulSoup(answer.text, "html.parser")
        a = soup.find_all('div', class_='card-body pt-0')[-1]
        name = a.find_next('h6', class_='card-title').text.replace('\n', '').strip()
        logger.info(f"Company name is {name}. INN is {taxpayer_id}")
        try:
            company_name: str = GoogleTranslator(source='uz', target='ru').translate(name[:4500])
        except Exception as e:
            logger.error(f"Exception is {e}")
            company_name = name
        self.cache_add_and_save(taxpayer_id, company_name, self.__str__())
        return company_name


class SearchEngineParser(BaseUnifiedCompanies):
    def __init__(self, unified_company):
        super().__init__()
        self.table_name = "search_engine"
        self.cur: sqlite3.Cursor = self.load_cache()
        self.unified_company = unified_company
        self.logger: logging.getLogger = get_logger(f"unified_companies {str(datetime.now().date())}")

    def is_valid(self, number: str) -> bool:
        pass

    def get_inn_from_site(self, dict_inn: dict, values: list, count_inn: int) -> None:
        """
        Parse each site (description and title).
        """
        unified_companies = [
            UnifiedRussianCompanies(),
            UnifiedKazakhstanCompanies(),
            UnifiedBelarusCompanies(),
            UnifiedUzbekistanCompanies()
        ]
        for item_inn in values:
            for unified_company in unified_companies:
                with contextlib.suppress(Exception):
                    if self.unified_company and self.unified_company.is_valid(item_inn):
                        dict_inn[item_inn] = dict_inn[item_inn] + 1 if item_inn in dict_inn else count_inn
                    if unified_company.is_valid(item_inn):
                        self.unified_company = unified_company
                        dict_inn[item_inn] = dict_inn[item_inn] + 1 if item_inn in dict_inn else count_inn

    def get_inn_from_html(self, myroot: ElemTree, index_page: int, results: int, dict_inn: dict, count_inn: int) \
            -> None:
        """
        Parsing the html page of the search engine with the found queries.
        """
        value: str = myroot[0][index_page][0][results][1][3][0].text
        title: str = myroot[0][index_page][0][results][1][1].text
        inn_text: list = re.findall(r"\d+", value)
        inn_title: list = re.findall(r"\d+", title)
        self.get_inn_from_site(dict_inn, inn_text, count_inn)
        self.get_inn_from_site(dict_inn, inn_title, count_inn)

    def get_code_error(self, error_code: ElemTree, value: str) -> None:
        """
        Getting error codes from xml_river.
        """
        if error_code.tag == 'error':
            code: Union[str, None] = error_code.attrib.get('code')
            message: str = MESSAGE_TEMPLATE.get(code, "Not found code error. Exception - {}. "
                                                      "Value - {}. Error code - {}")
            message = message.format(error_code.text, value, code)
            prefix: str = PREFIX_TEMPLATE.get(code, "необработанная_ошибка_на_строке_")
            self.logger.error(f"{message}. {prefix}")
            if code == '200':
                raise AssertionError(message)
            elif code == '110' or code != '15':
                self.logger.error(f"Error code is {code}")

    def parse_xml(self, response: Response, value: str) -> Tuple[ElemTree.Element, int, int]:
        """
        Parsing xml.
        """
        myroot: ElemTree = ElemTree.fromstring(response.text)
        self.get_code_error(myroot[0][0], value)
        index_page: int = 2 if myroot[0][1].tag == 'correct' else 1
        try:
            last_range: int = int(myroot[0][index_page][0][0].attrib['last'])
        except IndexError as index_err:
            self.logger.warning(f"The request to Yandex has been corrected, so we are shifting the index. "
                                f"Exception - {index_err}")
            index_page += + 1
            last_range = int(myroot[0][index_page][0][0].attrib['last'])
        return myroot, index_page, last_range

    def get_inn_from_search_engine(self, value: str) -> dict:
        """
        Looking for the INN in the search engine, and then we parse through the sites.
        """
        self.logger.info(f"Before request. Data is {value}")
        try:
            r: Response = requests.get(f"https://xmlriver.com/search_yandex/xml?user={USER_XML_RIVER}"
                                       f"&key={KEY_XML_RIVER}&query={value} ИНН", timeout=120)
        except Exception as e:
            self.logger.error(f"Run time out. Data is {value}. Exception is {e}")
            raise AssertionError from e
        self.logger.info(f"After request. Data is {value}")
        myroot, index_page, last_range = self.parse_xml(r, value)
        dict_inn: dict = {}
        count_inn: int = 1
        for results in range(1, last_range):
            try:
                self.get_inn_from_html(myroot, index_page, results, dict_inn, count_inn)
            except Exception as ex:
                self.logger.warning(f"Description {value} not found in the Yandex. Exception - {ex}")
        self.logger.info(f"Dictionary with INN is {dict_inn}. Data is {value}")
        return dict_inn

    def get_company_by_taxpayer_id(self, value: str):
        """
        Getting the INN from the cache, if there is one. Otherwise, we search in the search engine.
        """
        rows: sqlite3.Cursor = self.cur.execute(f'SELECT * FROM "{self.table_name}" WHERE taxpayer_id=?', (value,), )
        list_rows: list = list(rows)
        if list_rows and list_rows[0][1] != "None":
            self.logger.info(f"Data is {list_rows[0][0]}. INN is {list_rows[0][1]}")
            return list_rows[0][1], list_rows[0][2]
        api_inn: dict = self.get_inn_from_search_engine(value)
        for inn in api_inn.items():
            with contextlib.suppress(Exception):
                if api_inn == 'None':
                    sql_update_query: str = f"""Update {self.table_name} set value = ? where key = ?"""
                    data: Tuple[str, str] = (inn[1], value)
                    self.cur.execute(sql_update_query, data)
                    self.conn.commit()
            self.cache_add_and_save(value, inn[0], self.unified_company.__str__())
        return max(api_inn, default=None), self.unified_company


if __name__ == "__main__":
    print(UnifiedRussianCompanies().is_valid("192494228"))
    print(UnifiedKazakhstanCompanies().is_valid("192494228"))
    print(UnifiedBelarusCompanies().is_valid("192494228"))
    print(UnifiedUzbekistanCompanies().is_valid("192494228"))
