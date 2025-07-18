import os
import logging
import numpy as np
import pandas as pd
from typing import Tuple
from itertools import cycle
from logging.handlers import RotatingFileHandler

LOG_FORMAT: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
DATE_FTM: str = "%d/%B/%Y %H:%M:%S"
WAITING_TIME: int = 300

# os.environ["XL_IDP_ROOT_UNZIPPING"] = "."
# os.environ["XL_IDP_PATH_UNZIPPING"] = "/home/timur/sambashare/unzipping/upload"

COEFFICIENT_OF_HEADER_PROBABILITY: int = 20
LEN_COLUMNS_IN_ROW: int = 5

DESTINATION_STATION_LABELS: Tuple = (
    "Address/ Адрес/ 地址",
    "Address/ Адрес/"
)

BASE_DIRECTORIES: list = ["errors_excel", "done", "archives", "json", "done_excel", "errors"]

USER_XML_RIVER: str = "6390"
KEY_XML_RIVER: str = "e3b3ac2908b2a9e729f1671218c85e12cfe643b0"

DADATA_TOKEN: str = "3321a7103852f488c92dbbd926b2e554ad63fb49"
DADATA_SECRET: str = "3c905f3b5b6291c65d323e3b774e7b8f6d1b7919"

MESSAGE_TEMPLATE: dict = {
    '200': "The money ran out. Index is {}. Exception - {}. Value - {}",
    '110': "There are no free channels for data collection. Index is {}. Exception - {}. Value - {}",
    '111': "There are no free channels for data collection. Index is {}. Exception - {}. Value - {}",
    '15': "No results found in the search engine. Index is {}. Exception - {}. Value - {}"
}

PREFIX_TEMPLATE: dict = {
    '200': "закончились_деньги_на_строке_",
    '110': "нет_свободных_каналов_на_строке_",
    '111': "нет_свободных_каналов_на_строке_",
    '15': "не_найдено_результатов_"
}

PROXIES: list = [
    'http://user139922:oulrqa@166.1.108.56:6573',
    'http://user139922:oulrqa@166.1.108.146:6573',
    'http://user139922:oulrqa@166.1.29.178:6573',
    'http://user139922:oulrqa@166.1.108.52:6573',
    'http://user139922:oulrqa@194.169.161.227:6573',
    'http://user139922:oulrqa@194.169.161.208:6573',
    'http://user139922:oulrqa@185.198.233.102:6573',
    'http://user139922:oulrqa@185.198.233.213:6573'
]
CYCLED_PROXIES: cycle = cycle(PROXIES)


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


def get_file_handler(name: str) -> logging.FileHandler:
    log_dir_name: str = f"{get_my_env_var('XL_IDP_ROOT_UNZIPPING')}/logging"
    if not os.path.exists(log_dir_name):
        os.mkdir(log_dir_name)
    file_handler: RotatingFileHandler = RotatingFileHandler(
        filename=f"{log_dir_name}/{name}.log", mode='a', maxBytes=10.5 * pow(1024, 2),
        backupCount=3
    )
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FTM))
    return file_handler


def get_stream_handler():
    stream_handler: logging.StreamHandler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return stream_handler


def get_logger(name: str) -> logging.getLogger:
    logger: logging.getLogger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers.clear()
    logger.addHandler(get_file_handler(name))
    logger.addHandler(get_stream_handler())
    logger.setLevel(logging.INFO)
    return logger


def read_config_table(sheet):
    df = pd.read_excel(
        f"{get_my_env_var('XL_IDP_ROOT_UNZIPPING')}/unzipping_table.xlsx",
        dtype=str,
        sheet_name=sheet
    )
    df.replace({np.nan: None, "NaT": None}, inplace=True)
    headers = list(df.columns)
    return {
        header: list(df.iloc[:, i].dropna().tolist())
        for i, header in enumerate(headers)
    }


class MissingEnvironmentVariable(Exception):
    pass


DICT_LABELS: dict = read_config_table("labels_before_table")
HEADER_LABELS: list = list(DICT_LABELS)[:6]
DICT_HEADERS_COLUMN_ENG: dict = read_config_table("headers_table")
DICT_STATION: dict = read_config_table("station")
