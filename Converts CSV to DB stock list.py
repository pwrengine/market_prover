
import sqlite3
import csv

connection = sqlite3.connect('stock_list.db')
cursor = connection.cursor()
cursor.execute("""
CREATE TABLE if not exists stocks (
    Symbol TEXT)
""")

h = 0

row_holder = []
with open('TSX.csv') as g:
    reader = csv.reader(g)
    print('This is READER')
    print(reader)
    print(type(reader))
    for t in reader:
        if t != []:
            cursor.execute('INSERT INTO Stocks VALUES (?)', t[0:1])
connection.commit()

