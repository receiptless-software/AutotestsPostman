import random
import uuid
import json
import base64
import requests
from threading import Thread
import http.client, sys
from queue import Queue
import time
import pg

DBconn = pg.DB(host="postgres.dev.spentlabs.com", user="postgres", passwd="hasp513#burt", dbname="postgres")
userid1 = str(random.randint(1000, 100000000))
sessionDiscription = str(random.randint(1000, 100000000))
urlAuth = "http://dev.spentlabs.com/api/v1/auth/token"
urlOffers = "http://dev.spentlabs.com/api/v1/offers"
urlReadHash = "SELECT * FROM customer_users where user_id='" + userid1 + "'"
urlreadresult = "SELECT * FROM cashback_transaction where user_id='" + userid1 + "'"

print("concurrent = ")
concurrent=int(input())
print("txns = ")
TxnCount=int(input())
print("Setted requests = ", concurrent*TxnCount)


# concurrent = 100
# TxnCount = 2500

payload = "{\n    \"customerName\": \"testCustomer\",\n    \"apiKey\": \"testCustomer\",\n    \"apiSecret\": \"testCustomer\"\n\t\n}"
response = requests.request("POST", urlAuth, data=payload)
print('Auth response = ',response.status_code)
response = response.json()
Token = response['token']

payload = "{\n  \"search\": \"Button Test Merchant (Sandbox)\"\n }"
headers = {'Authorization': Token}
response = requests.request("POST", urlOffers, data=payload, headers=headers)
print('Offer response = ', response.status_code)
response = response.json()
OfferIdUB = response[0]['offers'][0]['offerId']
urlOfferlinkUB = response[0]['offers'][0]['trackingUrl'] + "&user=" + str(userid1)
response = requests.request("GET", urlOfferlinkUB)
print('Click response = ', response.status_code)

payload = "{\n  \"search\": \"Health Testing Centers\"\n }"
headers = {'Authorization': Token}
response = requests.request("POST", urlOffers, data=payload, headers=headers)
print('Offer response = ', response.status_code)
response = response.json()
OfferIdA = response[0]['offers'][0]['offerId']
urlOfferlinkA = response[0]['offers'][0]['trackingUrl'] + "&user=" + str(userid1)
response = requests.request("GET", urlOfferlinkA)
print('Click response = ', response.status_code)

payload = "{\n  \"search\": \"Zeelool1\"\n }"
headers = {'Authorization': Token}
response = requests.request("POST", urlOffers, data=payload, headers=headers)
print('Offer response = ', response.status_code)
response = response.json()
OfferIdC = response[0]['offers'][0]['offerId']
urlOfferlinkC = response[0]['offers'][0]['trackingUrl'] + "&user=" + str(userid1)
response = requests.request("GET", urlOfferlinkC)
print('Click response = ', response.status_code)

time.sleep(5)
response = DBconn.query(urlReadHash).getresult()
Hash = response[0][2]
EpochTS = int(round(time.time() * 1000))
Base64UB = base64.b64encode((Hash + '|' + OfferIdUB + '|' + str(EpochTS)).encode()).decode('utf-8')
Base64A = base64.b64encode((Hash + '|' + OfferIdA + '|' + str(EpochTS)).encode()).decode('utf-8')
Base64C = base64.b64encode((Hash + '|' + OfferIdC + '|' + str(EpochTS)).encode()).decode('utf-8')
print('Base64UB = ', Base64UB)
print('Base64A = ', Base64A)
print('Base64C = ', Base64C)



list  = ['pending','locked','rejected']
totalsum = 0
set = 1
k = 0
k1 = 0
k2 = 0

def doWork():
    global k,k1,k2,w,totalsum
    w=0
    while w<TxnCount:
        w += 1
        while True:
            body, conn, sumint = q.get()
            conn.request("POST", "/crap/txn", json.dumps(body))
            res = conn.getresponse()
            k2 += 1
            conn.close()
            if res.status == 200:
                totalsum = totalsum + sumint
                k = k + 1
            if k%100 == 0:
                print(k)
            else: k1 += 1
        # q.task_done() #This line turn on serial posting and line 103



q = Queue(concurrent*TxnCount)
start_time = time.time()
for i in range(concurrent):
    t = Thread(target=doWork)
    t.daemon = True
    t.start()

try:
    for body in range(0,  concurrent*TxnCount):
        if set == 3: #Here uou can set what partners will be used
            set = 1
        connUB = http.client.HTTPConnection('dev.spentlabs.com', 8502)
        connA = http.client.HTTPConnection('dev.spentlabs.com', 8501)
        connC = http.client.HTTPConnection('dev.spentlabs.com', 8504)
        userid = str(uuid.uuid1())
        status = random.choice(list)
        sumint = random.randint(1, 10000)
        sum1 = str(sumint*100)
        sum = str(sumint)
        if set == 1:
            conn = connUB
            payload = {
            "request_id": "attempt-XXX",
            "data": {
                "posting_rule_id": None,
                "order_currency": "USD",
                "modified_date": "2019-10-17T20:00:00.000Z",
                "created_date": "2019-10-16T20:00:00.000Z",
                "order_line_items": [
                    {
                        "identifier": "sku-1234",
                        "total": sum1,
                        "amount": 2000,
                        "quantity": 3,
                        "publisher_commission": 1000,
                        "sku": "sku-1234",
                        "upc": "400000000001",
                        "category": [
                            "Clothes"
                        ],
                        "description": "T-shirts",
                        "attributes": {
                            "size": "M"
                        }
                    }
                ],
                "button_id": "btn-XXX",
                "campaign_id": "camp-XXX",
                "rate_card_id": "ratecard-XXX",
                "order_id": "order-1",
                "customer_order_id": "abcdef-123456",
                "account_id": "acc-XXX",
                "btn_ref": "srctok-XXYYZZ",
                "currency": "USD",
                "pub_ref": "publisher-token",
                "status": status,
                "event_date": "2019-10-15T20:00:00Z",
                "order_total": sum1,
                "advertising_id": "aaaaaaaa-1111-3333-4444-999999999999",
                "publisher_organization": "org-XXX",
                "commerce_organization": sessionDiscription,
                "amount": 1000,
                "button_order_id": "btnorder-XXX",
                "publisher_customer_id": Base64UB,
                "id": str(round(time.time() * random.randint(1, 10000))),
                "order_click_channel": "app",
                "category": "new-user-order",
                "validated_date": "2019-10-18T19:02:09Z"
            },
            "id": "hook-XXX",
            "event_type": "tx-validated"
            }

        if set == 2:
            conn = connA
            payload = {
            "programName": "SPENT",
            "userEmail": "",
            "status": "pending",
            "uniqueRecordId": userid,
            "suppliedSubProgramId": "",
            "transactionId": 47112006,
            "storeOrderId": "9119973897547",
            "userId": "12313",
            "suppliedUserId": Base64A,
            "storeName": "Target",
            "tentativeCannotChangeAfterDate": 1536969600,
            "tentativeCannotChangeAfterDatetime": "2019-09-15T00:00:00.000Z",
            "timestamp": 1530569679,
            "datetime": "2019-07-02T22:14:39.000Z",
            "postDatetime": "2019-10-03T15:30:02.000Z",
            "sale": sum,
            "commission": 0.44,
            "userCommission": 0.33,
            "sourceType": "site",
            "resellerPayoutDate": None,
            "logoUrl": "https://s3.amazonaws.com/storeslogo/459720"
            }

        if set == 3:
            conn = connC
            payload = {
            "COMMISSIONID": "124008841",
            "COMMISSIONAMOUNT": 3,
            "NETWORKSTATUS": "approved",
            "ORDERID": "124008841",
            "SUBAFFILIATEID": Base64C,
            "ADVERTISERNAME": "Fanatics",
            "ID": int(round(time.time() * random.randint(1, 10000))),
            "NETWORK": "sas",
            "EVENTDATE": "05/07/2019 11:35 AM",
            "SALEAMOUNT": sum
            }


        set = set+1
        q.put((payload,conn,sumint))
        # q.join() #This line turn on serial posting and line 103

except socket.error as error:
    print("Connection Failed **BECAUSE:** {}").format(error)
except (Exception,KeyboardInterrupt):
    sys.exit(1)




i=0
TSum=0
while k2 < (concurrent*TxnCount)*0.985: #0.985 because some part of request finished with connection errors
    time.sleep(0.1)
endtime = time.time()
# endtime = time.time()
print("Total perf speed: %s r/s ---" % (k2/(endtime - start_time)))
print('Total summ send: ',(totalsum/100))
print('Send txns: ', k)
print('cicles: ', k2)
print('Failed txns: ', k1)
print('user_id = ',userid1)
# time.sleep((concurrent*TxnCount)*0.005)
item_dict = DBconn.query(urlreadresult).getresult()
TxnCount = len(item_dict)

while i < TxnCount:
    Amount = item_dict[i][11]
    TSum = TSum + float(Amount)
    i=i+1
print('writed txns: ', i)
print('Total summ send: ',totalsum)
print('Total summ read: ',TSum)
