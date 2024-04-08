import os
import logging
from typing import Dict, Tuple
from logging.handlers import RotatingFileHandler

LOG_FORMAT: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
DATE_FTM: str = "%d/%B/%Y %H:%M:%S"

os.environ['XL_IDP_PATH_UNZIPPING'] = '.'

COEFFICIENT_OF_HEADER_PROBABILITY: int = 20

PRIORITY_SHEETS: list = [
    "INVOICE- SPECIFICATION",
    "инвойс спецификация",
    "проф",
    "Проформа",
    "спецификаци",
    "инв-спецификация"
]

DICT_LABELS: dict = {
    (
        "Продавец",
        "Отправитель",
        "Отправитель / Продавец",
        "Продавец/Отправитель",
        "Shipper /Отправитель / 发货人",
        "Отправитель /Shipper",
        "The Shipper / Отправитель",
        "Shipper /Грузоотправитель",
        "Отправитель / Продавец Shipper / Seller",
        "Отправитель/Продавец",
        "Shipper /Отправитель",
        "Отправитель/The Sender 发货人",
        "Отправитель/Shipper",
        "Shipper/Отправитель",
        "Отправитель по станции/ Shipper in SMGS/ 运单发货人",
        "The Sender/ Отправитель",
        "Shipper / Отправитель",
        "Отправитель / Shipper",
        "Отправитель / shipper",
        "Продавец/ Отправитель"
    ):
        "seller",
    (
        "Shipper/Seller",
        "Seller/Продавец/ 卖家",
        "Продавец/Seller",
        "ПродавецSeller",
        "Seller/Продавец/",
        "Seller / Продавец",
        "Shipper / Seller",
        "The Seller / Продавец",
        "Seller/Продавец",
        "Продавец/The seller 卖方",
        "Consignor / Грузоотправитель/Продавец",
        "Продавец / Отправитель Seller/ Shipper",
        "Seller/ Продавец",
        "Продавец/Seller 卖家",
        "The Seller/ Продавец",
        "Seller/Продавец/ 卖方",
        "Seller/shipper",
        "Seller"
    ):
        "seller_priority",
    (
        "Покупатель",
    ):
        "buyer",
    (
        "Buyer",
        "Buyer / Покупатель/ 买主",
        "покупатель/Buyer",
        "ПокупательBuyer",
        "Buyer / Покупатель/",
        "Buyer / Покупатель",
        "Buyer /",
        "The Buyer / Покупатель",
        "The Buyer",
        "Покупатель/The buyer 买方",
        "Consignee / Грузополучатель/Покупатель",
        "Получатель/",
        "Покупатель/The buyer 买方",
        "Покупатель/Buyer 买家",
        "The Buyer / Покупатель The Consigneee/ Получатель",
        "The Buyer/ Покупатель",
        "Buyer/Покупатель",
        "Buyer / Покупатель/ 买方",
        "Buyer/Consignee"
    ):
        "buyer_priority",
    (
        "Станция назначения",
        "Place of delivery / Место доставки",
        "Станция назначения / Destination station",
        "Станция назначения (Destination station)",
        "Destination Station/ Станция назначения 目的站",
        "Станция назначения/目的车站",
        "Beneficiary",
        "Station of destination / Станция назначения"
    ):
        "destination_station"
}

DESTINATION_STATION_LABELS: Tuple = (
    "Address/ Адрес/ 地址",
)

DICT_HEADERS_COLUMN_ENG: Dict[Tuple, str] = {
    (
        "Модель",
    ):
        "model",
    (
        "No.",
        "No.编号",
        "Pll",
        "№ п/п",
        "No. / №",
        "Номер",
        "№",
        "No. / №",
        "Item Позиция"
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
        "Код ТН ВЭД",
        "Tariff code / Код товара в соответствии с ТН ВЭД",
        "HS Code / 海关编码 Код ТН ВЭД ТС",
        "Код ТН ВЭД ТС",
        "Код товара в соответствии с ТН ВЭД",
        "Код ТНВЭД",
        "HS Code Код ТН ВЭД ТС",
        "HS CODE ГНГ",
        "HS Code",
        "HS Code / Код ТН ВЭД ТС HS CODE",
        "HS CODE",
        "HS Code / Код ТН ВЭД ТС (10位数)",
        "HS Code / Код ТН ВЭД ТС (10位数）"
    ):
        "tnved_code",
    (
        "Страна происхождения товара",
        "Country of origin/ Страна происхождения товара/ 商品原产国",
        "Страна происхождения товара 货源国",
        "Country of origin / Страна происхождения товара",
        "Country of Origin / Страна происхождения",
        "Страна происхождения товара商品原产地",
        "Страна происх.товара",
        "Страна происхождения",
        "Страна происхождения товара 货源国(英文/俄文)"
    ):
        "country_of_origin",
    (
        "Описание товара",
        "Описание товаров",
        "Описание товаров/description of goods",
        "Description of goods/ Описание товаров/货物名称",
        "Description of goods",
        "Описание товаров/description of goods 货物名，描述",
        "Description of goods /Описание товаров",
        "Name of product / Наименование товара",
        "Описание товаров/description of goods货物名称",
        "Название",
        "Наименование товара",
        "Name of product / Наименование товара",
        "Description Описание товара",
        "Description of goods/ Описание товаров/RUS",
        "Описание товаров/description of goods 货物名，描述(英文/俄文) 织物、薄膜、卷装货物、玻璃等货物必须标注M2",
        "Наименование товара/Состав/размеры",
        "DESCRIPTION"
    ):
        "goods_description",
    (
        "Qty pcs / Кол-во шт",
        "Qty pcs / Кол-во шт/ 单件数量",
        "Qty pcs / Кол-во шт 单件数",
        "Кол-во, шт",
        "Q-ty, pcs / Кол-во, штук",
        "Quantity / Кол-во",
        "Кол-во шт",
        "Количество",
        "Кол-во",
        "Measurement units/ Ед. измерения",
        "quantity количество",
        "Number of pairs / Кол-во пар",
        "Qty pcs / Кол-во шт 单件数",
        "Q-ty, pcs / Кол-во в ед. измерения",
        "Q'TY"
    ):
        "quantity",
    (
        "Qty of packages/Кол-во мест, коробов",
        "Qty of packages/ Кол-во мест/ 包装数量",
        "Qty of packages/Кол-во мест, коробов 件数",
        "Qty places, boxes /Кол-во мест, коробок",
        "Quantity (meters) /Кол-во (метры)",
        "Кол-во, мест",
        "Number of units load (cll.) / Кол-во мест",
        "装箱数量Qty of packages/Кол-во мест",
        "Кол-во мест, коробов",
        "Количество штук",
        "Кол-во мест",
        "Number of packages/ Количество упаковок",
        "Qty of packages/Кол-во мест, коробок",
        "Number of pkgs Место",
        "Qty of packages/ Кол-во мест/ 包装数量",
        "Qty of packages/Кол-во мест, коробов 件数"
    ):
        "package_quantity",
    (
        "Net weight kg / Вес нетто кг",
        "Net weight kg / Вес нетто кг/ 净重",
        "Net weight kg / Вес нетто кг 净重",
        "Net weight, kg / Вес нетто, кг",
        "NETT weight, KG",
        "Вес НЕТТО,кг",
        "Net weight, kg / Вес нетто, кг",
        "净重Net weight kg / Вес нетто кг",
        "Вес нетто кг",
        "Вес нетто, кг",
        "Net weight, kg / Вес нетто, кг",
        "Net Weight (KG) Нетто",
        "Net weight kg / Вес нетто кг/ 净重",
        "Net weight, kg / Вес  нетто, кг."
    ):
        "net_weight",
    (
        "Gross weight kg / Вес брутто кг",
        "Gross weight kg / Вес брутто кг / 毛重",
        "Gross weight kg / Вес брутто кг 毛重",
        "Gross weight, kg /Вес брутто, кг",
        "GROSS weight, KG",
        "Вес БРУТТО,кг",
        "Gross weight, kg / Вес  брутто, кг",
        "毛重Gross weight kg / Вес брутто кг",
        "Вес брутто кг",
        "Вес брутто, кг",
        "Gross weight, kg / Вес брутто,кг",
        "Gross Weight (KG) Брутто",
        "Gross weight kg / Вес брутто кг / 毛重",
        "Gross weight, kg / Вес  брутто,кг."
    ):
        "gross_weight",
    (
        "Price per pcs / Цена за шт, CNY",
        "Price per one peace/Цена за шт/ 单件价格",
        "Price per 1 pcs / Цена за 1 шт, USD 单价",
        "Price per pcs /Цена за шт, CNY",
        "Price for unit, Chinese yuan / Цена за единицу, китайские юани",
        "Price USD / unit / Цена USD / ед.",
        "單價 / Price per unit / Цена за ед изм, USD",
        "Цена за шт, USD",
        "Цена, USD",
        "Price USD/unit of measure. / Цена, долл.США/ед.изм.",
        "Unit Price (PC) единичная цена",
        "Price per pair/Цена за пару/CNY",
        "Price USD/ Цена долл. США",
        "Unit Price (USD)"
    ):
        "price_per_piece",
    (
        "Amount / Общая стоимость, CNY",
        "Amount / Общая стоимость/ 总货值",
        "Amount / Общая стоимость, USD 总价",
        "Amount /Общая стоимость, CNY",
        "The Ammount Of Chinese yuan / Сумма китайские юани",
        "Amount, USD / Стоимость, USD",
        "总货值 / Amount / Общая стоимость, USD",
        "Общая стоимость, USD",
        "Сумма, USD",
        "Стоимость, $",
        "Amount, USD / Стоимость, долл.США",
        "Total Price Общая цена",
        "Amount / Общая стоимость/CNY",
        "Amount (USD)",
        "Amount / Общая стоимость,CNY"
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
    file_handler = RotatingFileHandler(filename=f"{log_dir_name}/{name}.log", mode='a', maxBytes=10.5 * pow(1024, 2),
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
