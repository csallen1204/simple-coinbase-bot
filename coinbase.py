import time
import os
import http.client
import json
import jwt
from cryptography.hazmat.primitives import serialization
import secrets
import threading

class coinbase:

    def __init__(self):
        self.ticketCounter = 1
        self.HEADERS = {
            'Content-Type': 'application/json',
            'cache-control': 'no-cache'
        }
        self.COINBASE_DOMAIN = "api.coinbase.com"
        self.COINBASE_API_PATH = "/api/v3/brokerage/"
        self.API_KEY=os.environ['COINBASE_API_KEY']
        self.API_SECRET=os.environ['COINBASE_API_SECRET']
        self.WAIT_INTERVAL = .28
        self.requestQueue = []
        self.responseQueue = []
        self.requestController = threading.Thread(target=self.procesRequestQueue)
        self.requestController.start()

    def restApiCall(self,method="GET",path="",payload={},parameters=""): 
        private_key = serialization.load_pem_private_key(self.API_SECRET.encode('utf-8'), password=None)
        jwt_token = jwt.encode(
            {
                'sub': self.API_KEY,
                'iss': "cdp",
                'nbf': int(time.time()),
                'exp': int(time.time()) + 120,
                'uri': f"{method} {self.COINBASE_DOMAIN}{self.COINBASE_API_PATH}{path}",
            },
            private_key,
            algorithm='ES256',
            headers={'kid': self.API_KEY, 'nonce': secrets.token_hex()},
        )
        self.HEADERS['Authorization'] = f"Bearer {jwt_token}"

        conn = http.client.HTTPSConnection(self.COINBASE_DOMAIN, 443)
        conn.request(method,self.COINBASE_API_PATH+path+parameters,json.dumps(payload),self.HEADERS)
        response = conn.getresponse()
        if response.status >= 200 and response.status <= 204:
            return (True, json.loads(response.read().decode()))
        return (False,None)
    
    def apiRequest(self,method="GET",path="",payload={},parameters=""):
        ticketNumber = self.addRequest(method,path,payload,parameters)
        while len(self.responseQueue) == 0:
            time.sleep(.01)
        while self.responseQueue[0]['ticket'] != ticketNumber:
                time.sleep(self.WAIT_INTERVAL)
        response = self.responseQueue[0]['response']
        self.responseQueue.pop(0)
        return response

    def addRequest(self,method,path,payload,parameters):
        self.requestQueue.append({'ticket': self.ticketCounter, 'method': method, 'path': path, 'payload': payload, 'parameters': parameters})
        if self.ticketCounter > 999999999:
            self.ticketCounter = 1
            return 999999999
        else:
            self.ticketCounter += 1
            return (self.ticketCounter - 1)
        
    def addResponse(self,ticketNumber,response):
        self.responseQueue.append({'ticket':ticketNumber,'response':response,'read': False})

    def procesRequestQueue(self):
        while True:
            if len(self.requestQueue) > 0:
                while len(self.requestQueue) > 0:
                    response = self.restApiCall(method = self.requestQueue[0]['method'],path=self.requestQueue[0]['path'],payload=self.requestQueue[0]['payload'],parameters=self.requestQueue[0]['parameters'])
                    if response[0]:
                        self.addResponse(self.requestQueue[0]['ticket'],response[1])
                        self.requestQueue.pop(0)
                    time.sleep(self.WAIT_INTERVAL)
            else:
                time.sleep(self.WAIT_INTERVAL)

    def processResponseQueue(self):
        while True:
            if len(self.responseQueue) > 0:
                while self.responseQueue[0]['read']:
                    self.responseQueue.pop(0)
            else:
                time.sleep(self.WAIT_INTERVAL)

    def getPairs(self):
        pairData = self.apiRequest(path="products")['products']
        pairList = []
        for i in range(0,len(pairData)):
            if pairData[i]['trading_disabled']:
                continue
            if pairData[i]['quote_display_symbol'] == 'USD' and '-USDC' not in pairData[i]['product_id'] and pairData[i]['volume_24h'] != '':
                if (float(pairData[i]['volume_24h']) * float(pairData[i]['price'])) > 999999.9:
                    pairList.append(pairData[i])
        return pairList
    
    def getCandles(self,pair,start=((int(time.time()))-21000),end=(int(time.time()))):
        return self.apiRequest(path=f"products/{pair}/candles",parameters=f"?start={start}&end={end}&granularity=ONE_MINUTE")['candles']

if __name__=="__main__":
    test = coinbase()
    stuff = test.getPairs()
