import os
import logging
from typing import Dict, Tuple
from logging.handlers import RotatingFileHandler

LOG_FORMAT: str = "[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s"
DATE_FTM: str = "%d/%B/%Y %H:%M:%S"

COEFFICIENT_OF_HEADER_PROBABILITY: int = 20

PRIORITY_SHEETS: list = [
    "INVOICE- SPECIFICATION",
    "INVOICE CPT SVILENGRAD",
    "Proforma Invoice",
    "ИНВ-проформа Китай",
    "Спецификация PR",
    "инвойс спецификация",
    "проф",
    "Проформа",
    "спецификаци",
    "спецификаци-1",
    "спецификация",
    "инв-спецификация",
    "инвойс"
]

DICT_LABELS: dict = {
    (
        "SHIPPER/ГРУЗООТПРАВИТЕЛЬ",
        "SHIPPER/ОТПРАВИТЕЛЬ",
        "SHIPPER/ОТПРАВИТЕЛЬ(SAMEASSMGS'SHIPPER)",
        "SHIPPER/ОТПРАВИТЕЛЬ/发货人",
        "SHIPPER（发货人）",
        "THECONSIGNOR/ОТПРАВИТЕЛЬ",
        "THESENDER/ОТПРАВИТЕЛЬ",
        "THESHIPPER/ОТПРАВИТЕЛЬ",
        "ГРУЗОТПРАВИТЕЛЬ/SHIPPER",
        "ОТПРАВИТЕЛЬ",
        "ОТПРАВИТЕЛЬ/SHIPPER",
        "ОТПРАВИТЕЛЬ/THESENDER",
        "ОТПРАВИТЕЛЬ/THESENDER发货人",
        "ОТПРАВИТЕЛЬ/ПРОДАВЕЦ",
        "ОТПРАВИТЕЛЬ/ПРОДАВЕЦSHIPPER/SELLER",
        "ОТПРАВИТЕЛЬПОСТАНЦИИ/SHIPPERINSMGS/运单发货人",
        "ПРОДАВЕЦ/ОТПРАВИТЕЛЬ",
    ):
        "seller",
    (
        "CONSIGNOR/ГРУЗООТПРАВИТЕЛЬ/ПРОДАВЕЦ",
        "SELLER",
        "SELLER/SHIPPER",
        "SELLER/ПРОДАВЕЦ",
        "SELLER/ПРОДАВЕЦ/",
        "SELLER/ПРОДАВЕЦ/卖家",
        "SELLER/ПРОДАВЕЦ/卖方",
        "SHIPPER/SELLER",
        "THESELLER/ПРОДАВЕЦ",
        "ПРОДАВЕЦ",
        "ПРОДАВЕЦ/",
        "ПРОДАВЕЦ/SELLER",
        "ПРОДАВЕЦ/SELLERОТПРАВИТЕЛЬ/SENDER",
        "ПРОДАВЕЦ/SELLER卖家",
        "ПРОДАВЕЦ/THESELLER",
        "ПРОДАВЕЦ/THESELLER卖方",
        "ПРОДАВЕЦ/ОТПРАВИТЕЛЬ",
        "ПРОДАВЕЦ/ОТПРАВИТЕЛЬSELLER/SHIPPER",
        "ПРОДАВЕЦSELLER",
    ):
        "seller_priority",
    (
        "ПОКУПАТЕЛЬ",
        "ПОКУПАТЕЛЬ/",
    ):
        "buyer",
    (
        "BUYER",
        "BUYER/",
        "BUYER/CONSIGNEE",
        "BUYER/ПОКУПАТЕЛЬ",
        "BUYER/ПОКУПАТЕЛЬ/",
        "BUYER/ПОКУПАТЕЛЬ/买主",
        "BUYER/ПОКУПАТЕЛЬ/买方",
        "CONSIGNEE/ГРУЗОПОЛУЧАТЕЛЬ/ПОКУПАТЕЛЬ",
        "THEBUYER",
        "THEBUYER/ПОКУПАТЕЛЬ",
        "THEBUYER/ПОКУПАТЕЛЬTHECONSIGNEEE/ПОЛУЧАТЕЛЬ",
        "ПОКУПАТЕЛЬ/BUYER",
        "ПОКУПАТЕЛЬ/BUYERПОЛУЧАТЕЛЬ/RECIPIENT",
        "ПОКУПАТЕЛЬ/BUYER买家",
        "ПОКУПАТЕЛЬ/THEBUYER",
        "ПОКУПАТЕЛЬ/THEBUYER买方",
        "ПОКУПАТЕЛЬ/ПОЛУЧАТЕЛЬ",
        "ПОКУПАТЕЛЬBUYER",
        "ПОЛУЧАТЕЛЬ/",
        "ПОЛУЧАТЕЛЬ/ПОКУПАТЕЛЬ",
    ):
        "buyer_priority",
    (
        "BENEFICIARY",
        "DESTINATIONSTATION",
        "DESTINATIONSTATION/",
        "DESTINATIONSTATION/СТАНЦИЯНАЗНАЧЕНИЯ",
        "DESTINATIONSTATION/СТАНЦИЯНАЗНАЧЕНИЯ目的站",
        "PLACEOFDELIVERY/МЕСТОДОСТАВКИ",
        "STATIONOFDESTINATION/СТАНЦИЯНАЗНАЧЕНИЯ",
        "STATIONOFDESTINATIONСТАНЦИЯНАЗНАЧЕНИЯ",
        "СТАНЦИЯНАЗНАЧЕНИЯ",
        "СТАНЦИЯНАЗНАЧЕНИЯ(DESTINATIONSTATION)",
        "СТАНЦИЯНАЗНАЧЕНИЯ/DESTINATIONSTATION",
        "СТАНЦИЯНАЗНАЧЕНИЯ/目的车站",
    ):
        "destination_station"
}

DESTINATION_STATION_LABELS: Tuple = (
    "Address/ Адрес/ 地址",
    "Address/ Адрес/ "
)

DICT_HEADERS_COLUMN_ENG: Dict[Tuple, str] = {
    (
        "МОДЕЛЬ",
    ):
        "model",
    (
        "ITEMПОЗИЦИЯ",
        "NO.",
        "NO./№",
        "NO.编号",
        "PLL",
        "НОМЕР",
        "№",
        "№П/П",
    ):
        "number_pp",
    (
        "CODEКОДТНВЭД",
        "HSCODE",
        "HSCODE/",
        "HSCODE/КОДТНВЭД",
        "HSCODE/КОДТНВЭДЕАЭС",
        "HSCODE/КОДТНВЭДТС",
        "HSCODE/КОДТНВЭДТС(10位数)",
        "HSCODE/КОДТНВЭДТС(10位数）",
        "HSCODE/КОДТНВЭДТСHSCODE",
        "HSCODE/КОДТНВЭДТС（国外对应的HSCODE）",
        "HSCODE/海关编码/КОДТНВЭДТСЕАЭС",
        "HSCODE/海关编码КОДТНВЭДТС",
        "HSCODERUSSIAКОДТНВЭДРОССИЯ",
        "HSCODEГНГ",
        "HSCODEКОДТНВЭДТС",
        "TARIFFCODE/КОДТОВАРАВСООТВЕТСТВИИСТНВЭД",
        "КОДТНВЭД",
        "КОДТНВЭДТС",
        "КОДТНВЭДТСЕАЭС",
        "КОДТОВАРАВСООТВЕТСТВИИСТНВЭД",
        "（清关HS编码）HSCODE/КОДТНВЭДТС",
    ):
        "tnved_code",
    (
        "COUNTRYOFORIGIN/СТРАНАПРОИСХОЖДЕНИЯ",
        "COUNTRYOFORIGIN/СТРАНАПРОИСХОЖДЕНИЯТОВАРА",
        "COUNTRYOFORIGIN/СТРАНАПРОИСХОЖДЕНИЯТОВАРА/商品原产国",
        "COUNTRYOFORIGINEСТРАНАПРОИСХОЖДЕНИЯ",
        "СТРАНАПРОИСХ.ТОВАРА",
        "СТРАНАПРОИСХОЖДЕНИЯ",
        "СТРАНАПРОИСХОЖДЕНИЯТОВАРА",
        "СТРАНАПРОИСХОЖДЕНИЯТОВАРА商品原产地",
        "СТРАНАПРОИСХОЖДЕНИЯТОВАРА货源国",
        "СТРАНАПРОИСХОЖДЕНИЯТОВАРА货源国(英文/俄文)",
    ):
        "country_of_origin",
    (
        "DESCRIPTION",
        "DESCRIPTIONOFGOODS",
        "DESCRIPTIONOFGOODS/ОПИСАНИЕТОВАРОВ",
        "DESCRIPTIONOFGOODS/ОПИСАНИЕТОВАРОВ/RUS",
        "DESCRIPTIONOFGOODS/ОПИСАНИЕТОВАРОВ/货物名称",
        "DESCRIPTIONНАИМЕНОВАНИЕТОВАРА",
        "DESCRIPTIONОПИСАНИЕТОВАРА",
        "NAMEOFPRODUCT/НАИМЕНОВАНИЕТОВАРА",
        "НАЗВАНИЕ",
        "НАИМЕНОВАНИЕ",
        "НАИМЕНОВАНИЕТОВАРА",
        "НАИМЕНОВАНИЕТОВАРА/СОСТАВ/РАЗМЕРЫ",
        "ОПИСАНИЕТОВАРА",
        "ОПИСАНИЕТОВАРОВ",
        "ОПИСАНИЕТОВАРОВ/DESCRIPTIONOFGOODS",
        "ОПИСАНИЕТОВАРОВ/DESCRIPTIONOFGOODS货物名称",
        "ОПИСАНИЕТОВАРОВ/DESCRIPTIONOFGOODS货物名，描述",
        "ОПИСАНИЕТОВАРОВ/DESCRIPTIONOFGOODS货物名，描述(英文/俄文)织物、薄膜、卷装货物、玻璃等货物必须标注M2",
        "ОПИСАНИЕТОВАРОВ/DESCRIPTIONOFGOODS（中英俄品名）",
    ):
        "goods_description",
    (
        "MEASUREMENTUNITS/ЕД.ИЗМЕРЕНИЯ",
        "NUMBEROFPAIRS/КОЛ-ВОПАР",
        "Q'TY",
        "Q-TY,PCS./КОЛ-ВОВЕД.ИЗМЕРЕНИЯ",
        "Q-TY,PCS/КОЛ-ВО,ШТУК",
        "Q-TY,PCS/КОЛ-ВОВЕД.ИЗМЕРЕНИЯ",
        "QTYPCS/КОЛ-ВОШТ",
        "QTYPCS/КОЛ-ВОШТ/单件数量",
        "QTYPCS/КОЛ-ВОШТ单件数",
        "QUANTITY(M)КОЛ-ВО(М)",
        "QUANTITY/КОЛ-ВО",
        "QUANTITYКОЛИЧЕСТВО",
        "КОЛ-ВО",
        "КОЛ-ВО,ШТ",
        "КОЛ-ВОШТ",
        "КОЛИЧЕСТВО",
        "（小件数)QTYPCS/КОЛ-ВОШТ",
    ):
        "quantity",
    (
        "(大件数）QTYOFPACKAGES/КОЛ-ВОМЕСТ,КОРОБОВ",
        "NUMBEROFPACKAGES/КОЛИЧЕСТВОУПАКОВОК",
        "NUMBEROFPKGSМЕСТО",
        "NUMBEROFUNITSLOAD(CLL.)/КОЛ-ВОМЕСТ",
        "QTYOFPACKAGES/КОЛ-ВОМЕСТ,КОРОБОВ",
        "QTYOFPACKAGES/КОЛ-ВОМЕСТ,КОРОБОВ件数",
        "QTYOFPACKAGES/КОЛ-ВОМЕСТ,КОРОБОК",
        "QTYOFPACKAGES/КОЛ-ВОМЕСТ/包装数量",
        "QTYPLACES,BOXES/КОЛ-ВОМЕСТ,КОРОБОК",
        "QUANTITY(METERS)/КОЛ-ВО(МЕТРЫ)",
        "QUANTITYPLACESКОЛ-ВОМЕСТ",
        "TOTALQ-TY,PCS/ОБЩЕЕКОЛ-ВОВЕД.ИЗМЕРЕНИЯ,ШТ",
        "КОЛ-ВО,МЕСТ",
        "КОЛ-ВОМЕСТ",
        "КОЛ-ВОМЕСТ,КОРОБОВ",
        "КОЛИЧЕСТВОШТУК",
        "装箱数量QTYOFPACKAGES/КОЛ-ВОМЕСТ",
    ):
        "package_quantity",
    (
        "NETTOWEIGHT,KGВЕСНЕТТО,КГ",
        "NETTWEIGHT,KG",
        "NETWEIGHT(KG)НЕТТО",
        "NETWEIGHT,KG/ВЕСНЕТТО,КГ",
        "NETWEIGHT,KG/ВЕСНЕТТО,КГ.",
        "NETWEIGHTKG/ВЕСНЕТТОКГ",
        "NETWEIGHTKG/ВЕСНЕТТОКГ/净重",
        "NETWEIGHTKG/ВЕСНЕТТОКГ净重",
        "ВЕСНЕТТО,КГ",
        "ВЕСНЕТТОКГ",
        "净重NETWEIGHTKG/ВЕСНЕТТОКГ",
        "（净重）NETWEIGHTKG/ВЕСНЕТТОКГ",
    ):
        "net_weight",
    (
        "GROSSWEIGHT(KG)БРУТТО",
        "GROSSWEIGHT,KG",
        "GROSSWEIGHT,KG/ВЕСБРУТТО,КГ",
        "GROSSWEIGHT,KG/ВЕСБРУТТО,КГ.",
        "GROSSWEIGHT,KGВЕСБРУТТО,КГ",
        "GROSSWEIGHTKG/ВЕСБРУТТОКГ",
        "GROSSWEIGHTKG/ВЕСБРУТТОКГ/毛重",
        "GROSSWEIGHTKG/ВЕСБРУТТОКГ毛重",
        "ВЕСБРУТТО,КГ",
        "ВЕСБРУТТОКГ",
        "毛重GROSSWEIGHTKG/ВЕСБРУТТОКГ",
        "（毛重)GROSSWEIGHTKG/ВЕСБРУТТОКГ",
    ):
        "gross_weight",
    (
        "PRICE100MПРАЙС100М",
        "PRICEFORUNIT,CHINESEYUAN/ЦЕНАЗАЕДИНИЦУ,КИТАЙСКИЕЮАНИ",
        "PRICEPER1PCS/ЦЕНАЗА1ШТ,USD单价",
        "PRICEPERONEPEACE/ЦЕНАЗАШТ/单件价格",
        "PRICEPERPAIR/ЦЕНАЗАПАРУ/CNY",
        "PRICEPERPCS/ЦЕНАЗАШТ,CNY",
        "PRICEUSD/UNIT/ЦЕНАUSD/ЕД.",
        "PRICEUSD/UNIT/ЦЕНАДОЛЛ.США/ЕДИНИЦУИЗМЕРЕНИЯ",
        "PRICEUSD/UNITOFMEASURE./ЦЕНА,ДОЛЛ.США/ЕД.ИЗМ.",
        "PRICEUSD/ЦЕНАДОЛЛ.США",
        "UNITPRICE(PC)ЕДИНИЧНАЯЦЕНА",
        "UNITPRICE(USD)",
        "UNITPRICE/ЦЕНАЗАЕД,USD",
        "ЦЕНА,USD",
        "ЦЕНАЗАШТ,USD",
        "單價/PRICEPERUNIT/ЦЕНАЗАЕДИЗМ,USD",
        "（单价）PRICEPERPCS/ЦЕНАЗАШТ,CNY",
    ):
        "price_per_piece",
    (
        "AMOUNT(USD)",
        "AMOUNT,USD/СТОИМОСТЬ,USD",
        "AMOUNT,USD/СТОИМОСТЬ,ДОЛЛ.США",
        "AMOUNT/ОБЩАЯСТОИМОСТЬ,CNY",
        "AMOUNT/ОБЩАЯСТОИМОСТЬ,USD总价",
        "AMOUNT/ОБЩАЯСТОИМОСТЬ/CNY",
        "AMOUNT/ОБЩАЯСТОИМОСТЬ/总货值",
        "AMOUNT/СТОИМОСТЬ,USD",
        "AMOUNTUSDОБШАЯСТОИМОСТЬUSD",
        "THEAMMOUNTOFCHINESEYUAN/СУММАКИТАЙСКИЕЮАНИ",
        "TOTALPRICEОБЩАЯЦЕНА",
        "ОБЩАЯСТОИМОСТЬ,USD",
        "СТОИМОСТЬ,$",
        "СУММА,USD",
        "总货值/AMOUNT/ОБЩАЯСТОИМОСТЬ,USD",
        "（总价）AMOUNT/ОБЩАЯСТОИМОСТЬ,CNY",
    ):
        "total_cost"
}


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


def get_file_handler(name: str) -> logging.FileHandler:
    log_dir_name: str = f"{get_my_env_var('XL_IDP_ROOT_UNZIPPING')}/logging"
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
