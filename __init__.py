import os
import logging
from typing import Dict, Tuple
from logging.handlers import RotatingFileHandler

LOG_FORMAT: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
DATE_FTM: str = "%d/%B/%Y %H:%M:%S"

os.environ['XL_IDP_PATH_UNZIPPING'] = '.'

COEFFICIENT_OF_HEADER_PROBABILITY: int = 20

DICT_LABELS: dict = {
    (
        "Отправитель",
        "Отправитель / Продавец",
        "Shipper/Seller",
        "Seller/Продавец/ 卖家",
        "Продавец/Отправитель",
        "Продавец/Seller",
        "Продавец\nSeller",
        "Seller/Продавец/",
        "Seller / Продавец",
        "Shipper / Seller"
    ):
        "sender",
    (
        "Покупатель",
        "Buyer",
        "Buyer / Покупатель/ 买主",
        "покупатель/Buyer",
        "Покупатель\nBuyer",
        "Buyer / Покупатель/",
        "Buyer / Покупатель",
        "Buyer /"
    ):
        "recipient",
    ("Станция назначения",): "destination_station",
    ("Станция отправления",): "departure_station",
}

DICT_HEADERS_COLUMN_ENG: Dict[Tuple, str] = {
    (
        "Модель",
    ):
        "model",
    (
        "No.",
        "No.编号",
        "Pll",
        "№ п/п"
    ):
        "number_pp",
    (
        "HS Code / Код ТН ВЭД ТС",
        "Код ТН ВЭД ТС ЕАЭС",
        "HS Code /海关编码/ Код ТН ВЭД ТС ЕАЭС",
        "HS Code /海关编码 / Код ТН ВЭД ТС ЕАЭС",
        "HS code RUSSIA КОД ТНВЭД РОССИЯ",
        "HS Code /",
        "HS Code /Код ТН ВЭД ТС",
        "HS code/Код ТНВЭД",
        "Код ТН ВЭД"
    ):
        "tnved_code",
    (
        "Страна происхождения товара",
        "Country of origin/ Страна происхождения товара/ 商品原产国",
        "Страна происхождения товара货源国",
        "Country of origin / Страна происхождения товара"
    ):
        "country_of_origin",
    (
        "Описание товара",
        "Описание товаров/description of goods",
        "Description of goods/ Описание товаров/货物名称",
        "Description of goods",
        "Описание товаров/description of goods货物名，描述",
        "Description of goods /Описание товаров",
    ):
        "goods_description",
    (
        "Qty pcs / Кол-во шт",
        "Qty pcs / Кол-во шт/ 单件数量",
        "Qty pcs / Кол-во шт单件数",
        "Кол-во, шт"
    ):
        "quantity",
    (
        "Qty of packages/Кол-во мест, коробов",
        "Qty of packages/ Кол-во мест/ 包装数量",
        "Qty of packages/Кол-во мест, коробов件数",
        "Qty places, boxes /Кол-во мест, коробок",
        "Quantity (meters) /Кол-во (метры)",
        "Кол-во, мест"
    ):
        "package_quantity",
    (
        "Net weight kg / Вес нетто кг",
        "Net weight kg / Вес нетто кг/ 净重",
        "Net weight kg / Вес нетто кг净重",
        "Net weight, kg / Вес нетто, кг",
        "NETT weight, KG",
        "Вес НЕТТО,кг"
    ):
        "net_weight",
    (
        "Gross weight kg / Вес брутто кг",
        "Gross weight kg / Вес брутто кг / 毛重",
        "Gross weight kg / Вес брутто кг毛重",
        "Gross weight, kg /Вес брутто, кг",
        "GROSS weight, KG",
        "Вес БРУТТО,кг"
    ):
        "gross_weight",
    (
        "Price per pcs / Цена за шт, CNY",
        "Price per one peace/Цена за шт/ 单件价格",
        "Price per 1 pcs / Цена за 1 шт, USD单价",
        "Price per pcs /Цена за шт, CNY",
        "Price for unit, Chinese yuan / Цена за единицу, китайские юани"
    ):
        "price_per_piece",
    (
        "Amount / Общая стоимость, CNY",
        "Amount / Общая стоимость/ 总货值",
        "Amount / Общая стоимость, USD总价",
        "Amount /Общая стоимость, CNY",
        "The Ammount Of Chinese yuan / Сумма китайские юани"
    ):
        "total_cost"
}


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


def get_file_handler(name: str) -> logging.FileHandler:
    log_dir_name: str = f"{get_my_env_var('XL_IDP_PATH_UNZIPPING')}/logging"
    if not os.path.exists(log_dir_name):
        os.mkdir(log_dir_name)
    file_handler = RotatingFileHandler(filename=f"{log_dir_name}/{name}.log", mode='a', maxBytes=1.5 * pow(1024, 2),
                                       backupCount=3)
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


class MissingEnvironmentVariable(Exception):
    pass
