from _sha256 import sha256
import pymongo


# client = pymongo.MongoClient("localhost", 27017)
# db = client.blockchain
#
# def digest_text(text):
#     s256 = sha256()
#     s256.update((text).encode())
#     return s256.hexdigest()

# genesis_block = {"seq_no": 0, "nonce": 0, "hash_phrase": digest_text("0" + "Genesis block"),
#                  "block_text": "Genesis block", "mine_duration": 0, "mine_time": datetime.now(), "previous_hash": "0"}
# db.blockinfo.insert_one(genesis_block)

# genesis_block2 = {"seq_no": 1, "nonce": 1000, "hash_phrase": digest_text("0" + "Genesis block2"),
#                  "block_text": "Genesis block2", "mine_duration": 10, "mine_time": datetime.now(), "previous_hash": "0"}
# db.blockinfo.insert_one(genesis_block2)
#
# genesis_block3 = {"seq_no": 2, "nonce": 10001, "hash_phrase": digest_text("0" + "Genesis block3"),
#                  "block_text": "Genesis block3", "mine_duration": 100, "mine_time": datetime.now(), "previous_hash": "0"}
# db.blockinfo.insert_one(genesis_block3)





client = pymongo.MongoClient("localhost", 27017)
db = client.blockchain

# question 1
print("# question 1")
x = db.blockinfo.find_one({"seq_no": 2})
print(x["nonce"])

# question 2
print("\n# question 2")
x = db.blockinfo.find_one(sort=[("mine_time", pymongo.ASCENDING)])
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
n_first_blocks = 2

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
