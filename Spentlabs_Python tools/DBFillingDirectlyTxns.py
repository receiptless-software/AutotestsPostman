import random
import uuid
import datetime
import json
import base64
import requests
from threading import Thread
import http.client, sys
from queue import Queue
import time
import pg

DBconn = pg.DB(host="postgres.dev.spentlabs.com", user="postgres", passwd="hasp513#burt", dbname="postgres")
insTxnUrl = "INSERT INTO cashback_transaction(id,reference,merchant_name,merchant_network,description,when_created,when_updated,purchase_date,purchase_amount,purchase_currency,cashback_base_usd,cashback_amount_usd,cashback_user_usd,cashback_own_usd,status,user_id,customer_name,raw_txn,offer_id,offer_timestamp) VALUES "
insQueurl = "INSERT INTO txn_delivery_queue(txn_id,customer_name,when_created,when_next_attempt,when_last_attempt,attempt_count,batch_id) VALUES "

start_time = time.time()
print(datetime.datetime.now())

userid1 = str(random.randint(1000, 100000000))
sessionDiscription = str(random.randint(1000, 100000000))
concurrent = 1000
TxnCount = 10000
usr = "DmitriyQA"
lst = []
k = 0
k1 = 0
k2 = 0
uschk = 0
userid = 0
list  = ['pending','available','rejected']
networklist = ['azigo','usebutton','mogl','coupilia']
totalsum = 0
fsum = 0
description = "test4"
def doWork():
    global k, k1, k2, w, totalsum
    w = 0
    while True:
        time.sleep(0.1)
        payload, payload2 = q.get()
        res = DBconn.query(insTxnUrl + payload)
        res2 = DBconn.query(insQueurl + payload2)
        k2 += 1
        print("complated: ", k2/10,"%")
        q.task_done()



q = Queue(concurrent * concurrent)
start_time = time.time()
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()
try:
    for payload in range(0,  concurrent):
        cnt = 0
        payload2 = ''
        payload = ''
        while cnt<TxnCount:
            if TxnCount>cnt>0:
                payload = payload + ','
                payload2 = payload2 + ','
            # while userid == uschk:
            id = str(uuid.uuid1())
            reference = str(uuid.uuid1())
            uschk = id
            status = random.choice(list)
            network = random.choice(networklist)
            sum = random.randint(1, 1000)
            cb = sum * 0.01
            fsum = fsum + sum
            TS = str(datetime.datetime.utcnow())
            payload = payload + str((id,reference,'Target',network,description,TS,TS,TS,sum,'USD',sum,cb,cb,cb,status,userid1,usr,'{}','71e6c6da-b2b7-11e8-8cf8-0e089bb5e710',TS))
            payload2 = payload2 + str((id,usr,TS,TS,TS,5,id))
            cnt += 1
        q.put((payload,payload2))
        q.join()
except KeyboardInterrupt:
    sys.exit(1)
i=0
TSum=0
TxnC = 0
endtime = time.time()
# endtime = time.time()
print("Total req speed: %s r/s ---" % (k2*TxnCount*2/(endtime - start_time)))
print('Total summ send: ',(totalsum/100))
print('Send txns: ', k)
print('cicles: ', k2)
print('Failed txns: ', k1)
print('user_id = ',userid1)