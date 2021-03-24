from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pymongo
from datetime import datetime
from hashlib import sha256

client = pymongo.MongoClient("localhost", 27017)
db = client.blockchain

max = 2 ** 32


def digest_text(seq_no, text, e, previous_hash):
    s256 = sha256()
    s256.update(str(seq_no).encode())
    s256.update(text.encode())
    s256.update(str(e).encode())
    s256.update(previous_hash.encode())
    return s256.hexdigest()


def validate(hashed, difficulty):
    return hashed[:difficulty] == difficulty * "0"


def find_max_id():
    x = db.blockinfo.find_one(sort=[("seq_no", pymongo.DESCENDING)])
    return x["seq_no"]


sc = SparkContext("local[*]", "blockChain")
sc.setLogLevel("WARN")


def generate_hash_phrase(seq_no, text, previous_hash, batch_size=10000, difficulty=3):
    res = []
    start = datetime.now()
    for i in range(0, max, batch_size):
        range_max = i + batch_size
        if range_max > max:
            range_max = max
        a = [x for x in range(i, range_max)]
        rdd = sc.parallelize(a).map(lambda e: (digest_text(seq_no, text, e, previous_hash), e)) \
            .filter(lambda f: validate(f[0], difficulty)).collect()
        print("rdd" + str(rdd) + "/ range -" + str(range_max))
        if len(rdd) > 0:
            res.append(rdd[0])
            break
    stop = datetime.now()
    hash_value = res[0][0]
    nonce = res[0][1]
    mine_duration = (stop - start).seconds
    return hash_value, nonce, mine_duration


genesis_hash_value, genesis_nonce, genesis_mine_duration = generate_hash_phrase(0, "Genesis block", "0")
genesis_block = {"seq_no": 0, "nonce": genesis_nonce, "mine_duration": genesis_mine_duration}
db.blockinfo.insert_one(genesis_block)
previous_hash_temp = genesis_hash_value


def load_to_mongo(rdd):
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


# Create a stream context with interval 15 seconds
ssc = StreamingContext(sc, 15)  # 120 secs
# Get lines of each interval
lines = ssc.socketTextStream('localhost', 9999)

# Generate DStream that concatenates all lines contained in a single batch
batch = lines.map(lambda x: ("block", x)).reduceByKey(lambda x, y: str(x) + str(y))
batch.pprint()
# Generate hash values and store to DB
batch.foreachRDD(load_to_mongo)

ssc.start()
ssc.awaitTermination()
