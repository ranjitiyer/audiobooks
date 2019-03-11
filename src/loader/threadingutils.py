# Concurrency with thread pools
from concurrent import futures

def task(input):
    raise Exception("Bad !!")

# def done_callback(fn: futures.Future):
#     if fn.cancelled():
#         print("Task was cancelled")
#     if fn.done():
#       error = fn.exception()
#       if error:
#           print(type(error))
#           # print(error)

all_futures = []
pool = futures.ThreadPoolExecutor(max_workers=1)
for i in range(0, 2):
    future = pool.submit(task, "input")
    all_futures.append(future)

for f in all_futures:
    if f.done():
        if f.exception() is None:
            print(f.result())
        else:
            print("Exception ")


