from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import pymongo
from datetime import datetime
from hashlib import sha256

xx = input("Give string:")


def digest_text(text):
    print(text)
    s256 = sha256()
    s256.update(text.encode())
    return s256.hexdigest()


def validate(hashed):
    print(hashed)
    return hashed[:3] == '000'


sc = SparkContext("local[*]", "dowJones")
sc.setLogLevel("WARN")

# a = [str(a) for a in range(2 ** 32)]
# aa = [digest_text(x+str(b)) for b in a]

# print(aa)
start = datetime.now()
a = [i for i in range(10000)]
rdd = sc.parallelize(a).map(lambda e: (digest_text(xx + str(e)), e)).filter(lambda f: f[0][:3] == '000').collect()



stop = datetime.now()
print(rdd)
print((stop - start).seconds)






