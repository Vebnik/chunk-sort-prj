# libs
import logging
import time

# module
from src.csv_parser import CsvRead
from src.data_parser import DataParser
from src.pd_data_praser import PdDataParser


def main():
  # logger config
  logging.basicConfig(level=logging.INFO)

  # get data in file
  csv = CsvRead('./data/all_stocks.csv')
  csv_itt = csv.get_csv_itt()

  # parsing data
  start = time.time()
  #parser = DataParser(csv_itt, chunk_size=50_000)
  pd_parser = PdDataParser(100_000, './data/all_stocks.csv')
  #res = parser.select_sorted(columns='high', limit=10)
  res = pd_parser.sort_data()

  print(f"Computed time: {time.time() - start}")
  print(res)
  
