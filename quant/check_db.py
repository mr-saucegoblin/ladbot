import os, psycopg2
from dotenv import load_dotenv
load_dotenv()
conn = psycopg2.connect(os.getenv('QUANT_DB_URL'))
cur = conn.cursor()
cur.execute('SELECT COUNT(*) FROM tickers')
print('tickers:', cur.fetchone()[0])
cur.execute('SELECT COUNT(*) FROM price_history')
print('price_history rows:', cur.fetchone()[0])
cur.execute("SELECT ticker FROM tickers WHERE ticker LIKE 'X%.TO' OR ticker LIKE 'Z%.TO' LIMIT 5")
etfs = cur.fetchall()
print('ETF spot check (should be empty):', etfs)
conn.close()
