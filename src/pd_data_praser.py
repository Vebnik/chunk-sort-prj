import pandas as pd
import csv
import itertools as it
import os
import time
import random as rnd


class PdDataParser:

  chunk_size = 100_000
  file_path: str
  fieldnames = ['date','open','high','low','close','volume','Name']

  def __init__(self, chunk_size: int, file_path: str) -> None:
    self.chunk_size == chunk_size
    self.file_path = file_path


  def get_df(self) -> pd.DataFrame:
    return pd.read_csv(self.file_path, chunksize=self.chunk_size)


  def sort_data(self) -> None:
    os.system(f'del dist\\*.csv')
    chunk_num = 0

    for chunk in self.get_df():
      results = chunk.sort_values('high')
      results.to_csv(f'./dist/{chunk_num}.csv', index=False)
      chunk_num+=1

    files_list = os.listdir(path='./dist')
    self._computed_sort(files_list)


  def _computed_sort(self, name_scope: list[str]) -> None:
    old_files = name_scope

    for num in range(0, len(name_scope), 2):
      try:

        print(f'./dist/{name_scope[num]}', 'and', f'./dist/{name_scope[num+1]}')

        file_a = open(f'./dist/{name_scope[num]}', 'r')
        file_b = open(f'./dist/{name_scope[num+1]}', 'r')
        file_merge = open(f'./dist/merge_{rnd.randint(0, 100)}.csv', 'w')

        iter_a = csv.DictReader(file_a)
        iter_b = csv.DictReader(file_b)

        merge = csv.DictWriter(file_merge, fieldnames=self.fieldnames)

        tmp_a = next(iter_a); tmp_b = next(iter_b); merge.writeheader()

        for i in it.count(start=0, step=1):
          if float(tmp_a.get('high') or 0) < float(tmp_b.get('high') or 0):
            merge.writerow(tmp_a)
            tmp_a = next(iter_a)
          elif float(tmp_a.get('high') or 0) > float(tmp_b.get('high') or 0):
            merge.writerow(tmp_b)
            tmp_b = next(iter_b)
          else:
            merge.writerow(tmp_a)
            merge.writerow(tmp_b)
            tmp_a = next(iter_a)
            tmp_b = next(iter_b)

        file_a.close()
        file_b.close()
        file_merge.close()

      except Exception as ex:
        if isinstance(ex, IndexError):

          file_a = open(f'./dist/{name_scope[-1]}', 'r')
          file_merge_out = open(f'./dist/merge_{rnd.randint(0, 100)}.csv', 'w')
          merge_out = csv.DictWriter(file_merge_out, fieldnames=self.fieldnames)
          iter_a = csv.DictReader(file_a)

          merge_out.writeheader()

          for row in iter_a:
            merge_out.writerow(row)

          file_merge_out.close()

        file_a.close()
        file_b.close()
        file_merge.close()

        print(ex)

    time.sleep(5)

    for file_name in old_files:
      os.system(f'del dist\\{file_name}')

    name_scope = os.listdir(path='./dist')

    print(name_scope)

    if len(name_scope) > 1:
      self._computed_sort(name_scope)

    exit()
    
