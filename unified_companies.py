import re
import abc
import time
import sqlite3
import requests
import contextlib
from __init__ import *
from pathlib import Path
from functools import reduce
from datetime import datetime
from bs4 import BeautifulSoup
from operator import add, mul
from stdnum.exceptions import *
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
            if unified_company.is_valid(company_data):
                yield unified_company

    @staticmethod
    def fetch_company_name(companies, taxpayer_id):
        for company in companies:
            if rows := company.cur.execute(
                f'SELECT * FROM "{company.table_name}" WHERE taxpayer_id=?',
                (taxpayer_id,),
            ).fetchall():
                yield rows[0][1], rows[0][2], rows[0][3]
            else:
                yield company.get_company_by_taxpayer_id(taxpayer_id, 3)


class UnifiedContextProcessor:
    @staticmethod
    def unified_values(context: dict):
        UnifiedContextProcessor.unify_station(context)
        UnifiedContextProcessor.unify_companies(context)

    @staticmethod
    def unify_station(context: dict):
        for value in DICT_STATION['station']:
            if value and value in context['destination_station'].upper():
                index = DICT_STATION['station'].index(value)
                context['destination_station'] = DICT_STATION['station_unified'][index]
                break

    @staticmethod
    def unify_companies(context: dict):
        manager = UnifiedCompaniesManager()

        for company in HEADER_LABELS[:4]:
            if company_data := context.get(company):
                taxpayer_id = UnifiedContextProcessor.extract_taxpayer_id(company_data, manager)
                context[f"{company}_taxpayer_id"] = taxpayer_id

                if taxpayer_id:
                    for unified_company in manager.unified_companies:
                        if unified_company := list(manager.get_valid_company(unified_company, taxpayer_id)):
                            company_names = list(manager.fetch_company_name(unified_company, taxpayer_id))
                            for company_name, phone_number, email in company_names:
                                if company_name is not None:
                                    context[f"{company}_unified"] = company_name
                                    context["phone_number"] = phone_number
                                    context["email"] = email

    @staticmethod
    def extract_taxpayer_id(company_data, manager):
        valid_company: Optional[object] = []
        all_digits = re.findall(r"\d+", company_data)

        for unified_company in manager.unified_companies:
            for item_inn in all_digits:
                if valid_company := list(manager.get_valid_company(unified_company, item_inn)):
                    return item_inn

        # If no valid taxpayer ID found, use search engine
        search_engine = SearchEngineParser(valid_company, manager)
        return search_engine.get_company_by_taxpayer_id(company_data, 3)


class BaseUnifiedCompanies(abc.ABC):
    def __init__(self):
        self.table_name: str = "cache_taxpayer_id"
        self.conn: sqlite3.Connection = sqlite3.connect(self.create_file_for_cache(), check_same_thread=False)
        self.cur: sqlite3.Cursor = self.load_cache()

    @abc.abstractmethod
    def is_valid(self, number: str) -> bool:
        pass

    @abc.abstractmethod
    def get_company_by_taxpayer_id(self, taxpayer_id: str, number_attempts: int) -> Optional[str]:
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
               phone_number TEXT,
               email TEXT,
               country TEXT)
            """)
        self.conn.commit()
        return cur

    @staticmethod
    def get_response(url, country, method="GET", data=None):
        # proxy: str = next(CYCLED_PROXIES)
        used_proxy: Optional[str] = None
        try:
            session: Session = requests.Session()
            # session.proxies = {"http": proxy}
            if method == "POST":
                response = session.post(url, json=data, timeout=120)
            else:
                response = session.get(url, timeout=120)
            logger.info(f"Статус запроса {response.status_code}. URL - {url}. Country - {country}")
            used_proxy = session.proxies.get('http')  # или 'https', в зависимости от протокола
            logger.info(f'Использованный прокси: {used_proxy}')
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred during the API request - {e}. Proxy - {used_proxy}.")

    def cache_add_and_save(
            self,
            taxpayer_id: str,
            company_name: str,
            phone_number: str = None,
            email: str = None,
            country: str = None
    ) -> None:
        """
        Saving and adding the result to the cache.
        """
        logger.info(f"Saving data to cache. Table is {self.table_name}. Data is {taxpayer_id}")
        self.cur.executemany(f"INSERT or REPLACE INTO {self.table_name} VALUES(?, ?, ?, ?, ?)",
                             [(taxpayer_id, company_name, phone_number, email, country)])
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

    def get_company_by_taxpayer_id(self, taxpayer_id: str, number_attempts: int):
        # sourcery skip: extract-method
        """
        Getting the company name unified from the cache, if there is one.
        Otherwise, we are looking for verification of legal entities on websites.
        :param taxpayer_id:
        :param number_attempts:
        :return:
        """
        if not (response := self.get_response(f"https://checko.ru/search?query={taxpayer_id}", self.__str__())):
            return None, None, None
        soup = BeautifulSoup(response.text, "html.parser")
        if company_name_tag := soup.find('h1', class_='mb-3'):
            company_name = company_name_tag.text.strip()
    
            phone_links = soup.find_all('a', href=lambda href: href and href.startswith('tel'))
            email_links = soup.find_all('a', href=lambda href: href and href.startswith('mailto'))
            list_phone_numbers = [phone_link.text.strip() for phone_link in phone_links]
            list_emails = [email_link.text.strip() for email_link in email_links]
    
            phone_number = "\n".join(list_phone_numbers) if list_phone_numbers else None
            email = "\n".join(list_emails) if list_emails else None
            logger.info(f"Company name is {company_name}. INN is {taxpayer_id}")
            self.cache_add_and_save(taxpayer_id, company_name, phone_number, email, self.__str__())
            return company_name, phone_number, email
        else:
            return None, None, None


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
        if len(number) != 12:
            return False
        w1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        w2 = [3, 4, 5, 6, 7, 8, 9, 10, 11, 1, 2]
        check_sum = self.multiply(w1, number) % 11
        if check_sum == 10:
            check_sum = self.multiply(w2, number) % 11
        return check_sum == int(number[-1])

    def get_company_by_taxpayer_id(self, taxpayer_id: str, number_attempts: int):
        # sourcery skip: extract-method
        """
        
        :param taxpayer_id:
        :param number_attempts:
        :return:
        """
        response_company = self.get_response(f"https://gateway.kompra.kz/company/{taxpayer_id}", self.__str__())
        response_contacts = self.get_response(f"https://gateway.kompra.kz/contacts/{taxpayer_id}", self.__str__())
        if response_company and response_contacts:
            company_name: str = response_company.json().get("name")
            response_json = response_contacts.json()
            phone_number = "\n".join(response_json.get('phones', [])) or None
            email = "\n".join(response_json.get('emails', [])) or None
            logger.info(f"Company name is {company_name}. BIN is {taxpayer_id}")
            self.cache_add_and_save(taxpayer_id, company_name, phone_number, email, self.__str__())
            return company_name, phone_number, email
        else:
            return None, None, None


class UnifiedBelarusCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()
        self.russian_companies = UnifiedRussianCompanies()

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

    def get_company_by_taxpayer_id(self, taxpayer_id: str, number_attempts: int):
        """
        
        :param taxpayer_id:
        :param number_attempts:
        :return:
        """
        return self.russian_companies.get_company_by_taxpayer_id(taxpayer_id, number_attempts)


class UnifiedUzbekistanCompanies(BaseUnifiedCompanies):
    def __init__(self):
        super().__init__()

    def __str__(self):
        return "uzbekistan"

    def __repr__(self):
        return "uzbekistan"

    def is_valid(self, number):
        return False if len(number) != 9 else bool(re.match(r'[3-8]', number))

    @staticmethod
    def decode_cf_email(encoded_email):
        r = int(encoded_email[:2], 16)
        return ''.join(
            chr(int(encoded_email[i:i + 2], 16) ^ r)
            for i in range(2, len(encoded_email), 2)
        )

    def get_company_by_taxpayer_id(self, taxpayer_id: str, number_attempts: int):
        """

        :param taxpayer_id:
        :param number_attempts:
        :return:
        """
        if not (response := self.get_response(f"http://orginfo.uz/en/search/all?q={taxpayer_id}", self.__str__())):
            return None, None, None
        soup = BeautifulSoup(response.text, "html.parser")
        if a := soup.find_all('a', class_='text-decoration-none og-card'):
            return self._extracted_from_get_company_by_taxpayer_id(a, taxpayer_id)
        else:
            return None, None, None

    def _extracted_from_get_company_by_taxpayer_id(self, a, taxpayer_id):
        link = a[0].get('href')
        response_link = requests.get(f"https://orginfo.uz{link}")
        soup_link = BeautifulSoup(response_link.text, 'html.parser')
        contacts = soup_link.find_all("a", class_="__cf_email__")
        element = [element['data-cfemail'] for element in contacts]
        company_name = soup_link.find('h1', class_='h1-seo').text
        phone_number = soup_link.find('a', class_='text-decoration-none text-body-hover text-success').text
        email = self.decode_cf_email(element[0]) if element else None
        logger.info(f"Company name is {company_name}. INN is {taxpayer_id}")
        try:
            company_name: str = GoogleTranslator(source='uz', target='ru').translate(company_name[:4500])
        except Exception as e:
            logger.error(f"Exception is {e}")
        self.cache_add_and_save(taxpayer_id, company_name, phone_number, email, self.__str__())
        return company_name, phone_number, email


class SearchEngineParser(BaseUnifiedCompanies):
    def __init__(self, country, manager):
        super().__init__()
        self.table_name = "search_engine"
        self.cur: sqlite3.Cursor = self.load_cache()
        self.country = country
        self.manager = manager

    def is_valid(self, number: str) -> bool:
        pass

    def get_inn_from_site(self, dict_inn: dict, values: list, count_inn: int) -> None:
        """
        Parse each site (description and title).
        """
        for item_inn in values:
            for unified_company in self.manager.unified_companies:
                countries = list(self.manager.get_valid_company(unified_company, item_inn))
                for country in countries:
                    self.country if country in self.country else self.country.append(country)
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

    def parse_xml(self, response: Response, value: str, dict_inn: dict, count_inn: int):
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
            self.get_inn_from_site(dict_inn, inn_text, count_inn)
            self.get_inn_from_site(dict_inn, inn_title, count_inn)

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
        self.parse_xml(r, value, dict_inn, count_inn)
        logger.info(f"Dictionary with INN is {dict_inn}. Data is {value}")
        return dict_inn

    def get_company_by_taxpayer_id(self, value: str, number_attempts: int):
        """
        Getting the INN from the cache, if there is one. Otherwise, we search in the search engine.
        """
        best_found_inn = None
        if number_attempts == 0:
            return best_found_inn, self.country
        unwanted_chars = r"[<>\«\»\’\‘\“\”`'\".,!@#$%^&*()\[\]{};?\|~=_+]+"
        value = re.sub(unwanted_chars, "", value)
        value = re.sub(" +", " ", value).strip()
        rows: sqlite3.Cursor = self.cur.execute(f'SELECT * FROM "{self.table_name}" WHERE taxpayer_id=?', (value,), )
        if (list_rows := list(rows)) and list_rows[0][1]:
            logger.info(f"Data is {list_rows[0][0]}. INN is {list_rows[0][1]}")
            return list_rows[0][1]
        try:
            api_inn: dict = self.get_inn_from_search_engine(value)
            best_found_inn = max(api_inn, key=api_inn.get, default=None)
            self.cache_add_and_save(
                value,
                best_found_inn,
                country=self.country.__str__()
            )
        except ConnectionRefusedError:
            time.sleep(60)
            self.get_company_by_taxpayer_id(value, number_attempts - 1)
        return best_found_inn


if __name__ == "__main__":
    import csv

    list_data_ = []
    with open('/home/timur/Загрузки/test_all_country (Копия).csv', mode='r') as file_:
        csvFile = csv.DictReader(file_)
        for lines in csvFile:
            dict_row = {"buyer": lines["company_name"]}
            UnifiedContextProcessor.unify_companies(dict_row)
            list_data_.append(dict_row)
    keys_ = list_data_[0].keys()
    with open("/home/timur/Загрузки/test_all_country2.csv", "w", newline="") as f_:
        dict_writer = csv.DictWriter(f_, keys_)
        dict_writer.writeheader()
        dict_writer.writerows(list_data_)
