import logging
import csv
import os


class DataParser:

  data_itt = None
  chunk_size = 100_000
  os_command = {
    'del_dir': lambda: os.system('rd /s /q dist'),
    'mk_dir': lambda: os.system('mkdir dist')
  }


  def __init__(self, data_itt: csv.DictReader, chunk_size: int) -> None:
    self.data_itt = data_itt
    self.chunk_size = chunk_size


  def save_file(self, data: list[dict], file_name: str) -> None:
    with open(f'./dist/{file_name}', 'w') as file:
      headers = ['date','open','high','low','close','volume','Name']

      writer = csv.DictWriter(file, fieldnames=headers)

      writer.writeheader()
      for row in data:
        writer.writerow(row)


  def _sorted(self, data: list[dict], sort_config: dict) -> list[dict]:
    try:
      data = [*filter(lambda el: el[sort_config.get('columns')], data)]

      return sorted(
        data, key=lambda el: el[sort_config.get('columns')]
        )[:sort_config.get('limit')]

    except Exception as ex:
      logging.critical(f'in _sorted{ex}')
      exit()


  def _sorted_compare(self, sort_config: dict, chunk_num: int) -> list[dict]:
    try:
      results = []

      for num in range(chunk_num):
        with open(f'./dist/{num}_chunk.csv') as file:
          results = [*results, *csv.DictReader(file)]

      data = self._sorted(results, sort_config)

      self.os_command.get('del_dir')()
      
      return data

    except Exception as ex:
      logging.info(f'in _sorted_compare {ex}')
      exit()


  def select_sorted(self, **sort_config) -> list[dict]:
    try:
      self.os_command.get('mk_dir')()

      temp_data = []; cnt_chunk = 0; chunk_num = 0

      for row in self.data_itt:
        if cnt_chunk == self.chunk_size:
          self.save_file(self._sorted(temp_data, sort_config), f'{chunk_num}_chunk.csv')

          cnt_chunk = 0; temp_data = []; chunk_num += 1

        temp_data.append(row)
        cnt_chunk += 1 

      if temp_data:
        self.save_file(self._sorted(temp_data, sort_config), f'{chunk_num+1}_chunk.csv')

      data = self._sorted_compare(sort_config, chunk_num)
      return data

    except Exception as ex:
      logging.critical(f'In select_sorted{ex}')
      exit()