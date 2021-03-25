import socket
import time
import random

# Server port set as constant
PORT = 9999

# Create socket connection at domain -> localhost and port -> 9999 | IP:PORT -> 127.0.0.1:9999
ssocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
ssocket.bind(('', PORT))
ssocket.listen()
print("Server ready: listening to port {0} for connections.\n".format(PORT))
(c, addr) = ssocket.accept()

# Initialize global counter to add suffix when reading file perpetually
counter = 0

# Open text file with utf-8 encoding for read-only operations
file = open('1661-0.txt', 'r', encoding='utf-8')

# Store all file lines in array prior to any other operation.
# This is not optimal solution but for the size of the file is acceptable
lines = file.readlines()

doc_len = len(lines)

while True:
    internal_counter = 0
    for l in lines:
        # Remove carriage return character from the end of each line
        l = l.strip()
        msg = l + " : suffix" + str(counter)
        internal_counter += 1
        # message string through the servers' socket
        c.send((msg + "\n").encode())
        print(msg)
        if internal_counter >= doc_len:
            counter += 1
        # Time randomization for the message emission from the server
        time.sleep(random.randint(2, 5))