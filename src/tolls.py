import csv


def miltiple_data(csv_path: str) -> None:
  headers = ['date','open','high','low','close','volume','Name']

  with open(csv_path, 'r+') as file:
    data = [*csv.DictReader(file)]*100

    writer = csv.DictWriter(file, fieldnames=headers)

    writer.writeheader()
    for row in data:
      writer.writerow(row)


miltiple_data('./data/all_stocks.csv')