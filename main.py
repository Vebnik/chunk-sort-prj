# libs
import logging
import time

# module
from src.csv_parser import CsvRead
from src.data_parser import DataParser


def main():
  # logger config
  logging.basicConfig(level=logging.INFO)

  # get data in file
  csv = CsvRead('./data/all_stocks_test.csv')
  csv_itt = csv.get_csv_itt()

  # parsing data
  start = time.time()
  parser = DataParser(csv_itt, chunk_size=1_000_000)
  res = parser.select_sorted(columns='high', limit=10)

  print(f"Computed time: {time.time() - start}")
  print(res)
  
