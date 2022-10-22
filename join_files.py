# Python 3.7.4 
import pandas as pd
import sqlite3 as sql 
from pathlib import Path

Path('sqlite_db.db').touch()

conn = sql.connect('sqlite_db.db')
c = conn.cursor()

users = pd.read_csv('Users.csv')
alerts = pd.read_csv('Alerts.csv')
tokens = pd.read_csv('token_id.csv')
towns = pd.read_csv('Towns.csv')
trx = pd.read_csv('Trx.csv')

users.to_sql('users', conn, if_exists='append', index = False)
alerts.to_sql('alerts', conn, if_exists='append', index = False)
tokens.to_sql('tokens', conn, if_exists='append', index = False)
towns.to_sql('towns', conn, if_exists='append', index = False)
trx.to_sql('trx', conn, if_exists='append', index = False)

table_first_merge = pd.read_sql('''SELECT * FROM trx t LEFT JOIN users u ON t.id_user = u.id_user''', conn)
table_first_merge = table_first_merge.loc[:,~table_first_merge.columns.duplicated()].copy()
table_first_merge.to_sql('table_first_merge', conn, if_exists='append', index = False)
MASTER = pd.read_sql('''SELECT * FROM table_first_merge t LEFT JOIN alerts a ON t.id_trx = a.id_trx''', conn)
column_names = ['id_trx', 'id_user', 'token_id', 'value', 'trx_city', 'ip_address',
                'first_name', 'last_name', 'email', 'gender', 'Town', 'alerts']
MASTER.columns = column_names
MASTER.to_sql('MASTER', conn, if_exists='append', index = False)

MASTER.to_excel("master.xlsx")
print("executed succesfully")