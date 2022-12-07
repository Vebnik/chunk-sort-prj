import logging
import csv
import os
import itertools as it


class DataParser:

  data_itt = None
  chunk_size = 100_000
  headers = ['date','open','high','low','close','volume','Name']
  os_command = {
    'del_dir': lambda: os.system('del /q /s dist'),
    'mk_dir': lambda: os.system('mkdir dist\prev & mkdir dist\\next')
  }


  def __init__(self, data_itt: csv.DictReader, chunk_size: int) -> None:
    self.data_itt = data_itt
    self.chunk_size = chunk_size


  def save_file(self, data: list[dict], file_name: str) -> None:
    with open(f'./dist/prev/{file_name}', 'w') as file:

      writer = csv.DictWriter(file, fieldnames=self.headers)

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


  def _sorted_compare(self, sort_config: dict, chunk_num: int, step=True) -> list[dict]:
    try:
      sum_sorted = 0
      cnt = 0
      print(f'Chunks {chunk_num}')

      for n1, n2 in zip([i for i in range(0,  chunk_num, 2)], [i for i in range(1, chunk_num+1, 2)]):
        print(f'open {n1}_chunk.csv and {n2}_chunk.csv')
        sum_sorted+= 1

        chunk_a = csv.DictReader(open(f'./dist/{"prev" if step else "next"}/{n1}_chunk.csv', 'r'))
        chunk_b = csv.DictReader(open(f'./dist/{"prev" if step else "next"}/{n2}_chunk.csv', 'r'))

        merge_chunk = csv.DictWriter(open(f'./dist/{"next" if step else "prev"}/{cnt}_chunk.csv', 'w'), fieldnames=self.headers)
        merge_chunk.writeheader()

        tmp_a = next(chunk_a)
        tmp_b = next(chunk_b, False)
        cnt+=1

        try:
          for i in it.count(start=0, step=1):
            if float(tmp_a.get('high') or 0) < float(tmp_b.get('high') or 0):
              merge_chunk.writerow(tmp_a)
              tmp_a = next(chunk_a, False)
            elif float(tmp_a.get('high') or 0) > float(tmp_b.get('high') or 0):
              merge_chunk.writerow(tmp_b)
              tmp_b = next(chunk_b, False)
            else:
              merge_chunk.writerow(tmp_a)
              merge_chunk.writerow(tmp_b)
              tmp_a = next(chunk_a, False)
              tmp_b = next(chunk_b, False)
        except: pass

      if sum_sorted < chunk_num:
        merge_chunk = csv.DictWriter(open(f'./dist/{"next" if step else "prev"}/{cnt}_chunk.csv', 'w'), fieldnames=self.headers)
        chunk_a = csv.DictReader(open(f'./dist/{"prev" if step else "next"}/{chunk_num}_chunk.csv', 'r'))
        merge_chunk.writeheader()

        for row in chunk_a:
          merge_chunk.writerow(row)

      if sum_sorted >= 2:
        return self._sorted_compare(sort_config, sum_sorted, not step)

      
    except Exception as ex:
      logging.info(f'in _sorted_compare {ex}')
      exit()


  def select_sorted(self, **sort_config) -> list[dict]:
    try:
      self.os_command.get('del_dir')()
      self.os_command.get('mk_dir')()

      temp_data = []; cnt_chunk = 0; chunk_num = 0

      for row in self.data_itt:
        if cnt_chunk == self.chunk_size:
          data_sort = sorted(temp_data, key=lambda el: float(el.get('high') or 0))
          self.save_file(data_sort, f'{chunk_num}_chunk.csv')

          cnt_chunk = 0; temp_data = []; chunk_num += 1

        temp_data.append(row)
        cnt_chunk += 1 

      if temp_data:
        temp_data = sorted(temp_data, key=lambda el: float(el.get('high') or 0))
        self.save_file(temp_data, f'{chunk_num}_chunk.csv')

      data = self._sorted_compare(sort_config, chunk_num)
      return data

    except Exception as ex:
      logging.critical(f'In select_sorted{ex}')
      exit()