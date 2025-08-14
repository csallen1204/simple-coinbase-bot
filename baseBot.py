from coinbase import coinbase
from dbTransactions import dbTransactions
import time
import threading

class baseBot:

    def __init__(self):
        self.cb = coinbase()
        self.db = dbTransactions()

    def buildHistory(self,pair,days=5):
        endTime = int(time.time())
        startTime = endTime - 21000
        candles = self.cb.getCandles(pair,startTime,endTime)
        candleCount = len(candles)
        while len(candles) > 0 and candleCount < (1440 * days):
            self.db.ingestCandles(pair,candles)
            endTime = startTime
            startTime = endTime - 21000
            candles = candles = self.cb.getCandles(pair,startTime,endTime)
            candleCount += len(candles)

    def buildEntireHistory(self,pair):
        endTime = int(time.time())
        startTime = endTime - 21000
        candles = self.cb.getCandles(pair,startTime,endTime)
        while len(candles) > 0:
            self.db.ingestCandles(pair,candles)
            endTime = startTime
            startTime = endTime - 21000
            candles = candles = self.cb.getCandles(pair,startTime,endTime)

    def getPairDataFrame(self,pair):
        #get 5 days worth of candles
        if self.db.tableExists(pair):
            self.db.dropTable(pair)
        self.buildHistory(pair)
        #create a dataframe for each pair's candles
        return self.db.tableToDataframe(pair)

def main():
    bot = baseBot()

    #loop
    while True:        
        pass
        #update pairs
        pairs = bot.cb.getPairs()

        #get 5 days of candles
        dataframes = {}
        for pair in pairs:
            dataframes[pair['product_id']] = bot.getPairDataFrame(pair['product_id'])

        #look at candles

        #establish websocket connections for monitoring opportunities

        #create buy and sell signals

        #start monitoring in real-time to look for buy/sell opportunities

        #keep track of buy/sell signals and the differences

if __name__=="__main__":
    main()
