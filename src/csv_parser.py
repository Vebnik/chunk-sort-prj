import logging
import csv


class CsvRead:

  file_path = None

  def __init__(self, file_path: str) -> None:
    self.file_path = file_path


  def get_csv_itt(self) -> csv.DictReader:
    try:
      file = open(self.file_path, 'r', encoding='utf-8')
      return csv.DictReader(file)
    except Exception as ex:
      logging.critical(f'In get_csv_itt{ex}')
      exit()