# SUMMARY HISTORY
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Set
import csv


# 1. Определение классов моделей
class AttributeItem:
    """Класс для представления атрибута объекта с историей изменений.
    Содержит:
    - id: уникальный идентификатор атрибута
    - objid: ссылка на объект, к которому относится атрибут
    - def_name: название атрибута
    - value: значение атрибута
    - created: дата создания атрибута
    - terminated: дата окончания действия атрибута"""

    def __init__(self, id: int, objid: int, def_name: str, value: str, created: datetime, terminated: datetime):
        self.id = id
        self.objid = objid
        self.def_name = def_name
        self.value = value
        self.created = created
        self.terminated = terminated


class ObjItem:
    """Класс для представления объекта с его характеристиками.
    Содержит:
    - id: уникальный идентификатор объекта
    - plant, scope, type, etype, eid: характеристики объекта
    - created: дата создания объекта
    - terminated: дата окончания действия объекта
    - attributes: список атрибутов объекта (экземпляров AttributeItem)"""

    def __init__(self, id: int, plant: str, scope: str, type: str, etype: str, eid: str,
                 created: datetime, terminated: datetime, attributes: List[AttributeItem] = None):
        self.id = id
        self.plant = plant
        self.scope = scope
        self.type = type
        self.etype = etype
        self.eid = eid
        self.created = created
        self.terminated = terminated
        self.attributes = attributes if attributes else []


class SummaryHistoryRecord:
    """Класс для представления итоговой записи истории изменений.
    Содержит все поля объекта плюс словарь атрибутов на конкретный момент времени."""

    def __init__(self, id: int, plant: str, scope: str, type: str, etype: str, eid: str,
                 created: datetime, terminated: Optional[datetime], attributes: Dict[str, str]):
        self.id = id
        self.plant = plant
        self.scope = scope
        self.type = type
        self.etype = etype
        self.eid = eid
        self.created = created
        self.terminated = terminated
        self.attributes = attributes


# 2. Функции для чтения данных из CSV
def parse_datetime(dt_str: str) -> datetime:
    """Парсит строку с датой в объект datetime.
    Обрабатывает специальное значение 'infinity' как максимальную дату."""
    if dt_str.lower() == 'infinity':
        return datetime(9999, 12, 31, 23, 59, 59)
    try:
        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S.%f')
    except ValueError:
        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')


def read_attributes_from_csv(file_path: str) -> List[AttributeItem]:
    """Читает атрибуты из CSV-файла и возвращает список объектов AttributeItem."""
    attributes = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            attr = AttributeItem(
                id=int(row['id']),
                objid=int(row['objid']),
                def_name=row['def'],
                value=row['value'],
                created=parse_datetime(row['created']),
                terminated=parse_datetime(row['terminated'])
            )
            attributes.append(attr)
    return attributes


def read_objects_from_csv(file_path: str) -> List[ObjItem]:
    """Читает объекты из CSV-файла и возвращает список объектов ObjItem."""
    objects = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            obj = ObjItem(
                id=int(row['id']),
                plant=row['plant'],
                scope=row['scope'],
                type=row['type'],
                etype=row['etype'],
                eid=row['eid'],
                created=parse_datetime(row['created']),
                terminated=parse_datetime(row['terminated'])
            )
            objects.append(obj)
    return objects


# 3. Логика для создания новых колонок
def get_all_attribute_names(attributes: List[AttributeItem]) -> Set[str]:
    """Возвращает множество уникальных имен атрибутов из всех объектов."""
    return {attr.def_name for attr in attributes}


# 4. Основная логика обработки
def process_data_to_dataframe(attributes: List[AttributeItem], objects: List[ObjItem]) -> pd.DataFrame:
    """Основная функция обработки данных. Создает DataFrame с историей изменений объектов и их атрибутов.

    Алгоритм работы:
    1. Собирает все уникальные имена атрибутов
    2. Создает шаблон DataFrame с нужными колонками
    3. Группирует атрибуты по объектам
    4. Для каждого объекта:
       - Находит все моменты изменений (даты создания/изменения атрибутов)
       - Для каждого периода между изменениями:
         - Определяет актуальные значения атрибутов
         - Создает запись в DataFrame
    """
    # Собираем все уникальные имена атрибутов
    all_attr_names = get_all_attribute_names(attributes)

    # Создаем DataFrame для хранения результатов
    columns = ['id', 'plant', 'scope', 'type', 'etype', 'eid', 'created', 'terminated'] + sorted(all_attr_names)
    result_df = pd.DataFrame(columns=columns)

    # Группируем атрибуты по объектам
    obj_attrs = {}
    for obj in objects:
        obj_attrs[obj.id] = [attr for attr in attributes if attr.objid == obj.id]

    # Обрабатываем каждый объект
    for obj_id, attrs in obj_attrs.items():
        # Находим объект по ID
        obj = next((o for o in objects if o.id == obj_id), None)
        if not obj:
            continue

        # Сортируем атрибуты по дате создания
        attrs_sorted = sorted(attrs, key=lambda x: x.created)

        # Собираем все уникальные даты изменений (атрибутов и самого объекта)
        change_dates = {attr.created for attr in attrs_sorted}
        change_dates.add(obj.created)
        if obj.terminated != datetime(9999, 12, 31, 23, 59, 59):
            change_dates.add(obj.terminated)

        # Сортируем даты изменений по возрастанию
        sorted_dates = sorted(change_dates)

        # Обрабатываем каждую дату изменений
        for i, date in enumerate(sorted_dates):
            # Дата окончания текущего периода - либо следующая дата изменения, либо terminated объекта
            terminated = sorted_dates[i + 1] if i < len(sorted_dates) - 1 else obj.terminated

            # Собираем актуальные атрибуты на текущую дату
            current_attrs = {}
            for attr in attrs_sorted:
                if attr.created <= date and (attr.terminated is None or attr.terminated >= date):
                    current_attrs[attr.def_name] = attr.value

            # Создаем запись для DataFrame
            record = {
                'id': obj.id,
                'plant': obj.plant,
                'scope': obj.scope,
                'type': obj.type,
                'etype': obj.etype,
                'eid': obj.eid,
                'created': date,
                'terminated': terminated,
                **current_attrs
            }

            # Добавляем запись в DataFrame
            result_df = pd.concat([result_df, pd.DataFrame([record])], ignore_index=True)

    return result_df


# Основная функция
def main():
    """Точка входа в программу. Выполняет:
    1. Чтение данных из CSV-файлов
    2. Обработку данных
    3. Сохранение результата в новый CSV-файл"""
    # Чтение данных из CSV
    attributes = read_attributes_from_csv('processed_attr.csv')
    objects = read_objects_from_csv('processed_obj.csv')

    # Обработка данных
    result_df = process_data_to_dataframe(attributes, objects)

    # Сохранение результата
    result_df.to_csv('history_summary.csv', index=False, encoding='utf-8')
    print("Результат сохранен в файл history_summary.csv")


if __name__ == "__main__":
    main()
