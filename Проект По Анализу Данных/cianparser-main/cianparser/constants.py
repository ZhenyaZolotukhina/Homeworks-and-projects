BASE_LINK = "https://cian.ru/cat.php?engine_version=2&p={}&region={}"
ACCOMMODATION_TYPE_PARAMETER = "&offer_type={}"
DURATION_TYPE_PARAMETER = "&type={}"
DEAL_TYPE = "&deal_type={}"

FLOATS_NUMBERS_REG_EXPRESSION = r"[+-]? *(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?"

ROOM = "&room{}=1"
STUDIO = "&room9=1"
WITHOUT_NEIGHBORS_OF_CITY = "&with_neighbors=0"
IS_ONLY_HOMEOWNER = "&is_by_homeowner=1"

CITIES = [['Москва', '1']]

NOT_STREET_ADDRESS_ELEMENTS = {"ЖК", "м.", "мкр.", "Жилой комплекс", "Жилой Комплекс"}

SPECIFIC_FIELDS_FOR_RENT_LONG = {"price_per_month", "commissions"}
SPECIFIC_FIELDS_FOR_RENT_SHORT = {"price_per_day"}
SPECIFIC_FIELDS_FOR_SALE = {"price", "residential_complex"}
