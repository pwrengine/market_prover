import sqlite3
import inflect


def str_create_table(x):
    correct_number = ''
    p = inflect.engine()
    number = p.number_to_words(x)
    for w in number:
        u = w
        if w == ' ':
            u = '_'
        elif w == '-':
            u = '_'
        correct_number = correct_number + u

    table_name = correct_number + '_day_avg'
    header = 'Volume'
    sql1 = 'CREATE TABLE IF NOT EXISTS ' + table_name
    sql1 = sql1 + '(Date TEXT,'
    sql1 = sql1 + 'Symbol TEXT,'
    sql1 = sql1 + header
    sql1 = sql1 + ', Close'
    sql1 = sql1 + ' FLOAT)'
    return sql1, header, table_name


conn = sqlite3.connect('first_try_1.db')
cur = conn.cursor()
list_of_avg_lengths = [4]
total_close = 0
for r in list_of_avg_lengths:
    tracer = 0
    row_str = []
    ticker_averages = []
    counter = r
    print('Collecting rolling averages for ' + str(counter))
    sql_tup = str_create_table(counter)
    sql = sql_tup[0]
    header = sql_tup[1]
    table_name = sql_tup[2]
    stored_rows = []

    cur.execute(sql)
    conn.commit()

    avg_len = counter
    total_volume = 0
    cur.execute("SELECT DISTINCT SYMBOL FROM Stocks")
    all_unique_stocks = cur.fetchall()

    for v in all_unique_stocks:
        cur.execute("SELECT Date, Close, Volume FROM Stocks WHERE Symbol=?", v) #added 'close' to the querie
        ind_ticker_list = cur.fetchall()
        total_lines = int(len(ind_ticker_list))
        total_volume = 0
# Note - close is [0][1], volume is [0][2], date is [0][0]

        if total_lines > r:
            while counter < total_lines:

                ticker = ''.join(str(v))
                ticker_l = len(ticker) - 2
                ticker = str(ticker[1:ticker_l])

                for i in range(avg_len, 0, -1):
                    full_row = ''.join(str(ind_ticker_list[counter - i]))
#                    print(full_row)
#                    print(type(full_row))


#                    row_len = len(full_row)
#                    print(row_len)
#                    vol_len = row_len - 1
#                    print(full_row)
#                    close_flt = (float((full_row[24:41])))
#                    print(close_flt)
#                    row_int = int(full_row[44:vol_len]) #to add in the close prices, need to adjust len
#                    print(row_int)
#                    print(ind_ticker_list[counter - i][1])
#                    print(ind_ticker_list[counter - i][2])

                    total_close = total_close + ind_ticker_list[counter - i + 1][1]
                    total_volume = total_volume + ind_ticker_list[counter - i +1][2]
#                    print(ind_ticker_list[counter - i][1])
#                    print(ind_ticker_list[counter - i][2])
#                    print(total_close)
#                    print(total_volume)

                average = total_volume/avg_len
                close_avg = total_close/avg_len
                total_volume = 0
                total_close = 0
#                holder = str(ind_ticker_list[counter])

                date_holder = ind_ticker_list[counter][0]
#                print(date_holder, ticker, average, close_avg)
#                quit()
                stored_rows.append([date_holder, ticker, average, close_avg])
                counter = counter + 1
                conn.commit()

            insert_sql = 'INSERT INTO '
            insert_sql = insert_sql + table_name + ' VALUES(?,?,?,?)'
            cur.executemany(insert_sql, stored_rows)
            stored_rows = []
            tracer = tracer + 1
            counter = 0
            average = 0
            close_avg = 0
            total_close = 0
            total_volume = 0
            conn.commit()
        print(ticker)
        if tracer >= 5:
            print('if triggered')
            quit()

