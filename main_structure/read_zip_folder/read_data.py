from main_structure.read_zip_folder.utils import get_file_names, os, logging, convert_from_zip_to_list, convert_from_string_to_int


class ReadAllFromZip:
    def __init__(self, date_from, date_to, type_, client_folder="NO_ID_FOUND", debug=False):
        self.date_from = date_from
        self.date_to = date_to
        self.type_ = type_
        self.client_folder = client_folder

        self.files_path = os.path.join('downloads', client_folder, f'{date_from}_{date_to}', type_)
        self.debug = debug

        self.any_errors = False

    def debug_(self, text):
        if self.debug:
            print(text)

    def read_files(self):
        all_list = []
        correct = []

        file_names = get_file_names(self.files_path)
        if not file_names:
            logging.warning(f"Не удалось получить список файлов по клиенту {self.client_folder}")
            self.debug_(f"Не удалось получить список файлов по клиенту {self.client_folder}")
            self.any_errors = True
            return

        for file_name in file_names:
            if file_name.endswith("zip"):
                path = os.path.join(self.files_path, file_name)
                if self.type_ in file_name:
                    data, cor = convert_from_zip_to_list(path)
                    if not data:
                        self.debug_(f"Что то могло пойти не так с файлом {file_name} чекай логи")


                    all_list += data
                    correct += cor

        if self.type_ == "PROMO":
            all_list = self.merge_promo(all_list)
        if self.type_ == "SKU":
            all_list = self.merge_sku(all_list)
        if not all_list:
            logging.warning(f"Не удалось объединить данные типа {self.type_} по маршруту {self.files_path}")
            self.debug_(f"Не удалось объединить данные типа {self.type_} по маршруту {self.files_path}")
            self.any_errors = True

            return
        return all_list, sum(correct)

    def merge_promo(self, list_items):
        try:
            dict_items = {}
            for item in list_items:
                item = convert_from_string_to_int(item, 6)
                if item[3] not in dict_items:
                    dict_items[item[3]] = item
                else:
                    count = item[-6]
                    price = item[-5]
                    rate = item[-1]
                    dict_items[item[3]][-6] += count
                    dict_items[item[3]][-5] += price
                    dict_items[item[3]][-1] += rate
            list_items = [item for item in dict_items.values()]
            return list_items
        except Exception as e:
            logging.error(f"Ошибка при объединении данных: {e}")
            self.debug_(f"Ошибка при объединении данных: {e}")
            return None

    def merge_sku(self, list_items):
        try:
            dict_items = {}
            for item in list_items:
                if item[0].isnumeric():
                    item = convert_from_string_to_int(item, 10)
                    if item[0] in dict_items:
                        count = item[-5]
                        clicks = item[-8]
                        views = item[-9]
                        dict_items[item[0]][-5] += count
                        dict_items[item[0]][-8] += clicks
                        dict_items[item[0]][-9] += views
                    else:
                        dict_items[item[0]] = item
            list_items = [item for item in dict_items.values()]
            return list_items
        except Exception as e:
            logging.error(f"Ошибка при объединении данных: {e}")
            self.debug_(f"Ошибка при объединении данных: {e}")
            return None
