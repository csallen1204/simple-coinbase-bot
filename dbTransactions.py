import sqlite3
import pandas

class dbTransactions:
    def __init__(self):
        pass

    def dropTable(self,pair):
        con = sqlite3.connect("datasets.db")
        cur = con.cursor()
        cur.execute(f"DROP TABLE '{pair}'")
        con.close()

    def tableExists(self,pair):
        con = sqlite3.connect("datasets.db")
        cur = con.cursor()
        cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{pair}'")
        result = cur.fetchone()
        con.close()
        return result is not None
    
    def getMostRecentCandleTime(self,pair):
        if not self.tableExists(pair):
            return False
        con = sqlite3.connect("datasets.db")
        cur = con.cursor()
        cur.execute(f"SELECT MAX(Start) FROM '{pair}' limit 1")
        result = cur.fetchone()[0]
        con.close()
        return result
    
    def getOldestCandleTime(self,pair):
        if not self.tableExists(pair):
            return False
        con = sqlite3.connect("datasets.db")
        cur = con.cursor()
        cur.execute(f"SELECT MIN(Start) FROM '{pair}' limit 1")
        result = cur.fetchone()[0]
        con.close()
        return result

    def ingestCandles(self,pair,candles):
        con = sqlite3.connect("datasets.db")
        cur = con.cursor()
        cur.execute("PRAGMA journal_mode = WAL")
        cur.execute(f"CREATE TABLE IF NOT EXISTS '{pair}' (Start INTEGER PRIMARY KEY, Low REAL, High REAL, Open REAL, Close REAL, Volume REAL)")
        
        for candle in candles:
            cur.execute(f"INSERT INTO '{pair}' VALUES({candle['start']}, {candle['low']}, {candle['high']}, {candle['open']}, {candle['close']}, {candle['volume']})")
        con.commit()
        con.close()
        return

    def tableToDataframe(self,pair,days=5):
        con = sqlite3.connect('datasets.db')
        df = pandas.read_sql_query(f"SELECT * FROM '{pair}' ORDER BY start DESC LIMIT {(1440 * days)}", con)
        df.index = pandas.DatetimeIndex(df['Start'])
        con.close()
        return df