from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pymongo
from datetime import datetime
from hashlib import sha256

client = pymongo.MongoClient("localhost", 27017)
db = client.blockchain
nonce = 4294967295
range = [x for x in range(nonce)]


def digest_text(text):
    s256 = sha256()
    s256.update((text).encode())
    return s256.hexdigest()


genesis_block = {"seq_no": 0, "nonce": 0, "hash_phrase": digest_text("0" + "Genesis block"),
                 "block_text": "Genesis block", "mine_duration": 0, "mine_time": datetime.now(), "previous_hash": "0"}
db.blockinfo.insert_one(genesis_block)


def find_max_id():
    x = db.blockinfo.find_one(sort=[("seq_no", pymongo.DESCENDING)])
    return x["seq_no"], x["previous_hash"]


def generate_hash_phrase(text):
    a = sc.parallelize(range)
    a2 = a.map(lambda x: )
    return hash, nonce2



def load_to_mongo(rdd):
    a = rdd.collect()
    text = a[0][1]
    seq_no, previous_hash = find_max_id()
    load_document = {}
    start = datetime.now()
    hash = generate_hash_phrase(previous_hash + text)
    stop = datetime.now()
    # mine_duration = (stop - start).seconds
    load_document["seq_no"] = seq_no + 1
    # load_document["nonce"] = nonce
    load_document["hash_phrase"] = generate_hash_phrase(previous_hash + text)
    load_document["block_text"] = text
    # load_document["mine_duration"] = mine_duration
    # load_document["mine_time"] = datetime.now()
    load_document["previous_hash"] = previous_hash
    # db.blockinfo.insert_one(load_document).inserted_id

    db.blockinfo.insert_one(load_document).inserted_id


def collectLines(rdd):
    pass


sc = SparkContext("local[*]", "dowJones")
sc.setLogLevel("WARN")
# Create a stream context with interval 15 seconds
ssc = StreamingContext(sc, 15)  # 120 secs
# Get lines of each interval
lines = ssc.socketTextStream('localhost', 9999)

# lines.pprint()

batch = lines.map(lambda x: ("block", x)).reduceByKey(lambda x, y: str(x) + str(y))
batch.pprint()

batch.foreachRDD(load_to_mongo)



# DB sanity
# db.blockinfo.insert_one({"tt": batch}).inserted_id
# x = db.blockinfo.find()
# print(x)
# DB sanity




ssc.start()
ssc.awaitTermination()
