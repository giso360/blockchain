from _sha256 import sha256
import pymongo

client = pymongo.MongoClient("localhost", 27017)
db = client.blockchain




# question 1
print("# question 1")
seq_no = 2

x = db.blockinfo.find_one({"seq_no": seq_no})
print(x["nonce"])

# question 2
print("\n# question 2")
x = db.blockinfo.find_one(sort=[("mine_duration", pymongo.ASCENDING)])
print(x)

# question 3
print("\n# question 3")
x = db.blockinfo.aggregate([
    {

        "$group": {

            "_id": 0,
            "avg_mine_duration": {

                "$avg": "$mine_duration"

            }

        }
    }
])
a = list(x)
print(a[0]['avg_mine_duration'])

# question 4
print("\n# question 4")
x = db.blockinfo.aggregate(
    [
        {

            "$group":

                {

                    "_id": "0",
                    "totalAmount": {

                        "$sum": "$mine_duration"

                    }

                }

        }
    ]
)
a = list(x)
print(a[0]["totalAmount"])

# question 5
print("\n# question 5")
upper_limit = 1002
lower_limit = 0

x = db.blockinfo.find(
    {

        "nonce": {
            "$gte": lower_limit,
            "$lt": upper_limit
        }

    }
)

a = list(x)
print(len(a))

# question 6: find total mining time of n_first_blocks
print("\n# question 6")

n_first_blocks = 100

x = db.blockinfo.aggregate(
    [

        {
            "$sort":
                {
                    "seq_no": 1
                }
        },

        {
            "$limit": n_first_blocks
        },

        {
            "$group":
                {
                    "_id": "0",
                    "totalAmount": {
                        "$sum": "$mine_duration"
                    }
                }
        }

    ]
)

a = list(x)
print(a[0]["totalAmount"])
