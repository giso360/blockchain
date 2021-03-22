import pymongo

client = pymongo.MongoClient("localhost", 27017)
db = client.blockchain

# # question 1
# x = db.blockinfo.find_one({"seq_no": 2})
# print(x["nonce"])
#
# # question 2
# x = db.blockinfo.find_one(sort=[("mine_time", pymongo.ASCENDING)])
# print(x)
#
# # question 3
# x = db.blockinfo.aggregate([
#     {
#         "$group": {
#             "_id": 0,
#             "avvvg": {
#                 "$avg": "$mine_time"
#             }
#         }
#     }
# ])
# print(x)
# a = list(x)
# print(a[0]['avvvg'])

# question 4
x = db.sales.aggregate(
    [
        {
            "$group":
                {
                    "_id": "0",
                    "totalAmount": {"$sum": "mine_time"},
                    "count": {"$sum": 1}
                }
        }
    ]
)
for i in x:
    print(i)
