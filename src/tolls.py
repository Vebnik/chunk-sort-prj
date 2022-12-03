import csv


def miltiple_data(csv_path: str) -> None:
  try:
    headers = ['date','open','high','low','close','volume','Name']
    row_sample = {'date': '2015-09-29', 'open': '1.67', 'high': '1.69', 'low': '1.65', 'close': '1.67', 'volume': '7005323', 'Name': 'AMD'}

    with open(csv_path, 'w+') as file:
      #data = [*csv.DictReader(file)]*2
      writer = csv.DictWriter(file, fieldnames=headers)
      
      writer.writeheader()
      # for row in data:
      for i in range(1_000_000_000):
        writer.writerow(row_sample)

  except Exception as ex:
    print(ex)

#miltiple_data('./data/all_stocks.csv')
miltiple_data('./data/all_stocks_test.csv')

