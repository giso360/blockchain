# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 18:30:40 2021

@author: MarousopoulouMaria
@author: SkoufiasGeorge
"""
print(__doc__)

import pymongo

client = pymongo.MongoClient("localhost", 27017)
db = client.blockchain


# question 1
print("# question 1: What is the nonce value of the block with a given sequence number")
seq_no = 2

x = db.blockinfo.find_one({"seq_no": seq_no})
print(x["nonce"])

# question 2
print("\n# question 2: Which block has the smallest mining time")
x = db.blockinfo.find_one(sort=[("mine_duration", pymongo.ASCENDING)])
print(x)

# question 3
print("\n# question 3: Which is the average mining time of all blocks mined so far")
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
print("\n# question 4: Which is the cumulative mining time of all blocks for a given range of block numbers")
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
print("\n# question 5: How many blocks have nonce value within a given range")
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

# question 6
print("\n# question 6: find total mining time of n_first_blocks")

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

# question 7
print("\n# question 3: Which is the average nonce value in the collection of mined blocks")
x = db.blockinfo.aggregate([
    {

        "$group": {

            "_id": 0,
            "avg_nonce": {

                "$avg": "$nonce"

            }

        }
    }
])
a = list(x)
print(round(a[0]['avg_nonce'], 2))
