import socket
import time
import random
import datetime

PORT = 9999

ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ssocket.bind(('', PORT))
ssocket.listen()
print("Server ready: listening to port {0} for connections.\n".format(PORT))
(c, addr) = ssocket.accept()

counter = 0

file = open('1661-0.txt', 'r', encoding='utf-8')
lines = file.readlines()
doc_len = len(lines)

while True:
    internal_counter = 0
    for l in lines:
        l = l.strip()
        msg = l + " : suffix" + str(counter)
        internal_counter += 1
        c.send((msg + "\n").encode())
        print(msg)
        if internal_counter >= doc_len:
            counter += 1
        time.sleep(random.randint(2, 5))