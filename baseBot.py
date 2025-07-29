from coinbase import coinbase
from dbTransactions import dbTransactions
import time

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

    def getInitialPairs(self):
        return self.cb.getPairs()

    def refreshPairs(self):
        pairs = self.cb.getPairs()
        
        #get 5 days worth of candles
        for item in pairs:
            self.buildHistory(item['product_id'])

        #get initial list of candidates, create a dataframe for each pair's candles and look for best opportunities
        dataframes = []
        from multiprocessing.dummy import Pool as ThreadPool
        pool = ThreadPool(16)
        results = pool.map(my_function, my_array)

def main():
    bot = baseBot()

    #loop
    while True:        
        pass
        #establish websocket connections for monitoring opportunities

        #create positions for best opportunities

        #start monitoring in real-time to look for buy/sell opportunities

        #keep track of buy/sell signals and the differences

if __name__=="__main__":
    main()
