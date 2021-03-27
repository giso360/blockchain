# -*- coding: utf-8 -*-
"""
Created on Sat Mar 20 17:20:36 2021

@author: MarousopoulouMaria
@author: SkoufiasGeorge
"""
print(__doc__)


from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pymongo
from datetime import datetime
from hashlib import sha256

# Initialize Mongo Client in pymongo by providing IP and port
client = pymongo.MongoClient("localhost", 27017)

# Define MongoDB database and collection
db = client.blockchain

# Maximum value for the nonce
max = 2 ** 32

# Number of leading zeros for the mining problem
mine_difficulty = 6


def digest_text(seq_no, text, e, previous_hash):
    """
    This method generates a hash value from the given inputs.
    Method used internally by generate_hash_phrase() method.
    :param seq_no: The sequence of the current block
    :param text: The transactions-strings contained in a batch concatenated
    :param e: The value of the nonce integer for the proof-of-work
    :param previous_hash: The hash value of the previous block
    :return:
    """
    s256 = sha256()
    s256.update(str(seq_no).encode())
    s256.update(text.encode())
    s256.update(str(e).encode())
    s256.update(previous_hash.encode())
    return s256.hexdigest()


def validate(hashed, difficulty):
    """
    This method is used to terminate the mining process regarding the achievement.
    Helper method used internally in generate_hash_phrase() method
    of the mining criterion with the leading zeros of the hash value
    :param hashed: The hash value obtained as a result of the digest_text()
    :param difficulty: Number of leading zeros for the mining problem
    :return: boolean - True | False
    """
    return hashed[:difficulty] == difficulty * "0"


def find_max_id():
    """
    This method is used to obtain the maximum sequence
    number of the blocks obtained from DB.
    Method used internally by load_to_mongo() service method
    :return: integer
    """
    x = db.blockinfo.find_one(sort=[("seq_no", pymongo.DESCENDING)])
    return x["seq_no"]


# create spark context using all available cores | provide appName as blockChain
sc = SparkContext("local[*]", "blockChain")

# allow ONLY warning level logs to be printed to console
# sc.setLogLevel("WARN")
sc.setLogLevel("OFF")


def generate_hash_phrase(seq_no, text, previous_hash, batch_size=1000000, difficulty=mine_difficulty):
    """
    This method is used to obtain the blocks' hash value alongside the nonce and the
    respective mine duration.
    Method used internally by load_to_mongo() service method.
    :param seq_no: The sequence number of the current block
    :param text: The concatenated text of the current batch
    :param previous_hash: The hash value of the previous block
    :param batch_size: The step used in the parallelization process
    :param difficulty: The mining difficulty used
    :return: The generated hash value, the nonce and the mine duration that meet the mining criterion
    """
    res = []
    start = datetime.now()
    for i in range(0, max, batch_size):
        range_max = i + batch_size
        if range_max > max:
            range_max = max
        a = [x for x in range(i, range_max)]
        rdd = sc.parallelize(a).map(lambda e: (digest_text(seq_no, text, e, previous_hash), e)) \
            .filter(lambda f: validate(f[0], difficulty)).collect()
        if len(rdd) > 0:
            res.append(rdd[0])
            break
    stop = datetime.now()
    hash_value = res[0][0]
    nonce = res[0][1]
    mine_duration = (stop - start).seconds
    return hash_value, nonce, mine_duration

# Create all necessary parameters for genesis block having: seq_no -> 0, transaction_text -> "Genesis block"
# and previous_hash value -> "0"
genesis_hash_value, genesis_nonce, genesis_mine_duration = generate_hash_phrase(0, "Genesis block", "0")

# Create python dictionary with all available data for Mongo insert of the genesis block
genesis_block = {"seq_no": 0, "nonce": genesis_nonce, "mine_duration": genesis_mine_duration}

# Insert genesis block data to MongoDB
db.blockinfo.insert_one(genesis_block)

# Initialize previous_hash value in order to avoid persisting it in MongoDB documents
previous_hash_temp = genesis_hash_value


def load_to_mongo(rdd):
    """
    This is the main service method used during batch processing.
    It is responsible for making internal calls to find_max()
    and generate_hash_phrase() methods to obtain all values to
    be stored in MongoDB after block processing has finalized.
    Subsequently, it stores the documents to DB.
    :param rdd: master rdd created during batch streaming
    :return: None
    """
    global previous_hash_temp
    a = rdd.collect()
    text = a[0][1]
    seq_no = find_max_id() + 1
    load_document = {}
    hash_value, nonce, mine_duration = generate_hash_phrase(seq_no, text, previous_hash_temp)
    load_document["seq_no"] = seq_no
    load_document["nonce"] = nonce
    load_document["mine_duration"] = mine_duration
    db.blockinfo.insert_one(load_document).inserted_id
    previous_hash_temp = hash_value


# Create a stream context with interval 120 seconds
# to meet project requirements
# OR 15 seconds to run
# investigative tasks of mining metadata w.r.t
# level of difficulty using the enumerations => [3, 5, 7]
ssc = StreamingContext(sc, 15)
# ssc = StreamingContext(sc, 120)

# Get lines of each interval
lines = ssc.socketTextStream('localhost', 9999)

# Generate DStream that concatenates all lines contained in a single batch
# by grouping them using the key string "block"
batch = lines.map(lambda x: ("block", x)).reduceByKey(lambda x, y: str(x) + str(y))

# Print batch with single RDD to client console
batch.pprint()

# Generate hash values and store to DB by calling load_to_mongo service method
# The arguments of the method call are omitted as they are inferred upon execution
batch.foreachRDD(load_to_mongo)

# Start streaming context
ssc.start()

# wait for keyboard event to terminate process
ssc.awaitTermination()
