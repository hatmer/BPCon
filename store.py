import pickle
import time

start = time.time()

value = "key,value"
val_bytes = value.encode('utf-8')
print(val_bytes)
print(len(val_bytes))
val_ints = int.from_bytes(val_bytes, byteorder='little')
print(val_ints)

# sent over network here

bytes_again = val_ints.to_bytes(9, byteorder='little')
print(bytes_again)
print(bytes_again.decode('utf-8'))

end = time.time()
print("Execution time: {}".format(end-start))



