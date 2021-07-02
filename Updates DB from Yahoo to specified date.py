# prior to this script, convert the text file to a DB with the full list of stocks

from pandas_datareader import data as pdr
import sqlite3
import datetime
from datetime import date

conn = sqlite3.connect('first_try.db')
list_con = sqlite3.connect('stock_list.db')
cur = conn.cursor()
cur2 = list_con.cursor()

cur.execute('SELECT MAX(date) FROM Stocks')
latest_date = cur.fetchone()
print('The last DB entry is on:')
print(latest_date)
current_date = date.today()
latest_year = int(latest_date[0][0:4])
latest_month = int(latest_date[0][5:7])
latest_day = int(latest_date[0][8:10])
print('The current date is:')
print(current_date)
working_date = date(latest_year, latest_month, latest_day)
working_date = working_date + datetime.timedelta(days=1)
print('The date to start Yahoo data grab is:')
print(working_date)


cur2.execute('SELECT * FROM Stocks')
symbol_list = cur2.fetchall()
for ticker in symbol_list:
    symbol = ticker[0]
    try:
        data = pdr.get_data_yahoo(symbol, start=working_date, end=date.today())
        data['Symbol'] = symbol
        data.to_sql('stocks', con=conn, schema='none', if_exists=""'append'"", index=True)
    except:
        print('Data grab failed for ' + symbol)
