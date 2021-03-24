from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pymongo
from datetime import datetime
from hashlib import sha256








difficulty = 3
zeros = difficulty * "0"
max_no = 1000000
min_no = 0
batch_size = 10000

xx = input("Give string:")


def digest_text(text):
    s256 = sha256()
    s256.update(str(0).encode())
    s256.update(text.encode())
    s256.update("0".encode())
    return s256.hexdigest()


def validate(hashed):
    return hashed[:difficulty] == zeros


sc = SparkContext("local[*]", "dowJones")
sc.setLogLevel("WARN")

res = []

start = datetime.now()

for i in range(min_no, max_no, batch_size):
    range_max = i + batch_size
    if range_max > max_no:
        range_max = max_no
    a = [x for x in range(i, range_max)]
    rdd = sc.parallelize(a).map(lambda e: (digest_text(xx + str(e)), e)).filter(lambda f: validate(f[0])).collect()
    if len(rdd) > 0:
        res.append(rdd[0])
        break

stop = datetime.now()

#
# a = [i for i in range(10000)]
# rdd = sc.parallelize(a).map(lambda e: (digest_text(xx + str(e)), e)).filter(lambda f: f[0][:3] == '000').collect()

hash_value = res[0][0]
nonce = res[0][1]
mine_duration = (stop - start).seconds

print(hash_value)
print(nonce)
print(mine_duration)


# print(res)
# print((stop - start).seconds)
