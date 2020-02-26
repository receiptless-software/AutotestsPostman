import random
import uuid
import json
from threading import Thread
import http.client, sys
from queue import Queue
import time
import pg

#Test can post txns to processor on dev.spentlabs.com:8600/crap/txns (but only serial send support is present because random UUID problem, when i start in parallel mode - i get same UUID  (not random) and i dont know reason of that)

# DBconn = pg.DB(host="spent-b2b-staging.cwmk7rmhhzlq.eu-west-1.rds.amazonaws.com", user="postgres", passwd="9Ez1Bu8ILD%nC*8khEHH4jTg", dbname="postgres")
DBconn = pg.DB(host="postgres.dev.spentlabs.com", user="postgres", passwd="hasp513#burt", dbname="postgres")
userid1 = str(random.randint(1000, 100000000))
sessionDiscription = str(random.randint(1000, 100000000))
urlAuth = "http://dev.spentlabs.com/api/v1/auth/token"
urlOffers = "http://dev.spentlabs.com/api/v1/offers"
urlReadHash = "SELECT * FROM customer_users where user_id='" + userid1 + "'"
urlreadresult = "SELECT * FROM cashback_transaction where user_id='" + userid1 + "'"
print('user_id = ',userid1)


concurrent = 10
TxnCount = 10
lst = []
k = 0
k1 = 0
k2 = 0
uschk = 0
userid = 0
list  = ['pending','available','rejected']
totalsum = 0
fsum = 0
def doWork():
    global k, k1, k2, w, totalsum
    w = 0
    # while w < TxnCount:
    #     w += 1
    while True:
        time.sleep(0.1)
        lst,conn,fsum = q.get()
        print(lst)
        # print(payload)
        # print(json.dumps(lst))
        conn.request("POST", "/crap/txns", json.dumps(lst))
        res = conn.getresponse()
        k2 += 1
        conn.close()
        # print(fsum)
        # print(lst)
        if res.status == 200:
            totalsum = totalsum + fsum
            k = k + 1
            print(k)
        else:
            k1 += 1
        # q.task_done()



q = Queue(concurrent * concurrent)
start_time = time.time()
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()
try:
    for body in range(0,  concurrent):
        conn = http.client.HTTPConnection('dev.spentlabs.com', 8600)
        cnt = 0
        # lst.clear()
        # while cnt<TxnCount:
        while userid==uschk:
            userid = str(uuid.uuid1())
        # userid = random.randint(1000000, 100000000)
        reference = str(random.randint(1000000, 100000000))
        uschk = userid
        # print(userid)
        status = random.choice(list)
        sum = random.randint(1, 1000)
        fsum = fsum + sum
        payload = [{
        "id" : userid,
        "userId" : userid1,
        "customerName" : "testCustomer",
        "reference" : reference,
        "merchantName" : "org-XXX1",
        "merchantNetwork" : "usebutton",
        "description" : "org-XXX",
        "whenCreated" : 1571256000000,
        "whenUpdated" : 1571342400000,
        "whenClaimed" : None,
        "whenSettled" : 1573759678118,
        "whenPosted" : None,
        "whenReceived" : 1573759678109,
        "purchaseDate" : 1571169600000,
        "purchaseAmount" : sum,
        "purchaseCurrency" : "USD",
        "cashbackBaseUSD" : sum,
        "cashbackTotalUSD" : 10.06,
        "cashbackUserUSD" : 1.01,
        "cashbackOwnUSD" : 9.05,
        "status" : status,
        "parentTxn" : None,
        "payoutId" : None,
        "failedReason" : None,
        "rawTxn" : {},
        "offerId" : "org-228b55a5707de5c8",
        "offerTimestamp" : 1573134478726
        }]
        # print(payload)
        # lst.append(payload)
            # cnt += 1
        # print(lst)
        q.put((payload,conn,fsum))
        # print(q.get(lst))
        # q.join()
except KeyboardInterrupt:
    sys.exit(1)
i=0
TSum=0
TxnC = 0
while k2 < (concurrent)*0.985:
    time.sleep(0.1)
endtime = time.time()
# endtime = time.time()
print("Total req speed: %s r/s ---" % (k2/(endtime - start_time)))
print('Total summ send: ',(totalsum/100))
print('Send txns: ', k)
print('cicles: ', k2)
print('Failed txns: ', k1)
print('user_id = ',userid1)
while TxnC < k2*TxnCount:
    item_dict = DBconn.query(urlreadresult).getresult()
    TxnC = len(item_dict)
    DBtime = time.time()
print("Total DB speed: %s r/s ---" % (k2*TxnCount/(DBtime - start_time)))
while i < TxnCount:
    Amount = item_dict[i][11]
    TSum = TSum + float(Amount)
    i=i+1
print('writed txns: ', i)
print('Total summ send: ',totalsum)
print('Total summ read: ',TSum)