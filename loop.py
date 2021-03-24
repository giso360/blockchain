#create range
# max_no = 2**32
# min_no = 0
# batch_size = 10000

# max_no = 2**4
max_no = 100
min_no = 0
batch_size = 50

for i in range(min_no, max_no, batch_size):
    range_max = i+batch_size
    if range_max > max_no:
        range_max = max_no
    a = [x for x in range(i, range_max)]
    print(a)