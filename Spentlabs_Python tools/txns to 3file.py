import random
import time
import json
import base64
import requests
from threading import Thread
import http.client, sys
from queue import Queue
import time


userid1 = str(random.randint(1000, 100000000))
sessionDiscription = str(random.randint(1000, 100000000))
urlAuth = "http://api-staging.spentlabs.com/api/v1/auth/token"
urlOffers = "http://api-staging.spentlabs.com/api/v1/offers"
urlReadHash = "http://localhost:3000/customer_users?user_id=eq." + userid1
urlreadresult = "http://localhost:3000/cashback_transaction?user_id=eq." + userid1
print('user_id = ',userid1)


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
response = requests.request("GET", urlReadHash)
print('DB response = ', response.status_code)
response = response.json()
Hash = response[0]['hash']
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
i=0
fileUB = open("TxnsUB.json", "w")
fileA = open("TxnsA.json", "w")
fileC = open("TxnsC.json", "w")
fileUB.truncate(0)
fileA.truncate(0)
fileC.truncate(0)
while i<30000:
    status = random.choice(list)
    sum = random.randint(1, 10000)
    userid = str(random.randint(1000, 100000000))
    sum1 = str(sum*100)
    sum = str(sum)
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
        "id": userid,
        "order_click_channel": "app",
        "category": "new-user-order",
        "validated_date": "2019-10-18T19:02:09Z"
    },
    "id": "hook-XXX",
    "event_type": "tx-validated"
    }
    n = fileUB.write('\n'+json.dumps(payload))
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
    n = fileA.write('\n'+json.dumps(payload))
    payload = {
    "COMMISSIONID": "124008841",
    "COMMISSIONAMOUNT": 3,
    "NETWORKSTATUS": "approved",
    "ORDERID": "124008841",
    "SUBAFFILIATEID": Base64C,
    "ADVERTISERNAME": "Fanatics",
    "ID": userid,
    "NETWORK": "sas",
    "EVENTDATE": "05/07/2019 11:35 AM",
    "SALEAMOUNT": sum
    }
    n = fileC.write('\n'+json.dumps(payload))
    i+=1

fileUB.close()
fileA.close()
fileC.close()
print('Finished:',i)
