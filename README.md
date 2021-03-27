# Blockchain Project

The scope of the present project is to demonstrate the usage of blockchain technologies based on the proof-of-work approach.



## <u>Files:</u>
### server.py:

The responsibility of this unit is to perpetually emit text lines from 1661-0.txt and make them available through a socket operating at port 9999

------

### client.py:

The responsibility of this unit is to listen to the socket connection. Using PySpark Streaming, the emitted lines are processed to generate a hash value that meets the requirements of the difficulty set. For instance, with level of difficulty = 3, valid hash values are those that acquire 3 leading zeros. For the hashing process, the following data are digested:

- The sequence number
- The text transactions
- The value of the previous hash
- An integer ranging from 0...4294967295
The mining process metadata for each block are stored to a MongoDB.  

------

### queries.py:

The responsibility of this unit is to perform queries on the stored collection: blockchain.blockinfo

------

### mini_f.txt:

This is small text file against which the functionality of the server (server.py) is demonstrated

------

### requirements.txt:

Set up venv and issue command: pip install -r requirements.txt


