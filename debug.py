import threading
import time


def print_text(text, sleep_time):
    time.sleep(3)
    print("thread is running")
    time.sleep(sleep_time)
    print(text)


threads = []
for i in range(3):
    t = threading.Thread(target=print_text, args=(f'Thread {i}', 3))
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print('All threads have finished.')
