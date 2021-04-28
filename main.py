# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

from typing import List, Callable, Coroutine, Dict, Any, Iterator, Optional
from matplotlib import pyplot as plt
import time


def chunked_filter(
        filter_fn: Callable[[List[Any]], Any] = lambda x: x,
        consumer: Optional[Coroutine] = None,
        batch_size: int = 0,
):
    """Coroutine which buffers input values up to batch_size, then filters the
    buffer through filter_fn, then feeds the filtered buffer to consumer."""
    values = []
    if consumer:
        consumer.__next__()
    try:
        while True:
            values.append((yield))
            if len(values) == batch_size:
                filtered = filter_fn(values)
                if filtered and consumer:
                    for value in filtered:
                        consumer.send(value)
                values = []
    except GeneratorExit:
        pass
    if values:
        filtered = filter_fn(values) if filter_fn else values
        if filtered and consumer:
            for value in filtered:
                consumer.send(value)
    if consumer:
        consumer.close()


def batch_index_comparator(
        browse_iterator: Iterator[Dict[str, Any]] = None,
        filter_fn: Callable[[List[Any]], Any] = None,
        consume_fn: Callable[[List[Any]], Any] = None,
        browse_batch_size: int = 0,
        consume_batch_size: int = 0,
):
    """Iterate over values from browse_iterator in chunks of size
    browse_batch_size, filter each chunk (array) through filter_fn and pass the
    filtered values to consume_fn as arrays of size consume_batch_size."""

    consumer = chunked_filter(filter_fn=consume_fn, batch_size=consume_batch_size)
    comparator = chunked_filter(filter_fn=filter_fn, consumer=consumer, batch_size=browse_batch_size)
    comparator.__next__()
    for item in browse_iterator:
        comparator.send(item)
    comparator.close()


ITEM_COUNT = 10000
SCALE_CONSUMER = 30
SCALE_GENERATOR = 10000000

def give(batch):
    batch_size = len(batch)
    real_delay = (abs(batch_size - 2000) / 1000 + 2) / SCALE_CONSUMER
    # print(f'consuming... {batch_size} in {real_delay} s')
    time.sleep(real_delay)
    return batch_size


def item_gen():
    for i in range(ITEM_COUNT):
        real_delay = 1 / SCALE_GENERATOR
        time.sleep(real_delay)
        yield {'dummy': 123}


def batch_gen(item_iter, batch_size):
    values = []
    for item in item_iter:
        values.append(item)
        if len(values) == batch_size:
            yield values
            values = []
    if len(values):
        yield values

def processSame2(browse_size=1000):
    start_time = time.time()
    iterator = batch_gen(item_gen(), browse_size)
    for batch in iterator:
        give(batch)
    delay = time.time() - start_time
    print(f'Delay same2 {delay}s')

def processBatching(browse_size=1000, consume_size=2000):
    start_time = time.time()
    iterator = batch_gen(item_gen(), browse_size)
    values = []
    for batch in iterator:
        values += batch
        if len(values) >= consume_size:
            count = give(values)
            # print(f'Consumed: {count}')
            values = []
    if len(values):
        count = give(values)
    delay = time.time() - start_time
    #print(f'Delay batch {delay}s')
    return delay


def processChunked(browse_size=1000, consume_size=2000):
    start_time = time.time()

    batch_index_comparator(browse_iterator=batch_gen(item_gen(), 1000), filter_fn=None, consume_fn=give, browse_batch_size=browse_size,
                           consume_batch_size=consume_size)
    delay = time.time() - start_time
    #print(f'Delay chunked {delay}s')
    return delay

def compare(c,b):
    return processChunked(c,b) - processBatching(c,b)



def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.
    # processSame2(1000)
    x = []
    y = []
    for i in range(1000, 1200, 100):
        x.append(i)
        consume_delay = (abs(4000-i - 2000) / 1000 + 2) / SCALE_CONSUMER
        gen_delay = i * 1/SCALE_GENERATOR
        diff = compare(i, 4000 - i)
        y.append(diff)

        print(f'Diff: {diff}')
        print(f'Consume delay : {consume_delay}')
        print(f'Generator delay : {gen_delay}')
    draw(x, y)

    print('End')




def draw(x,y):
    plt.title("Compare ")
    plt.xlabel("Browse size (consume size in range(3000,1000))")
    plt.ylabel("Diff chunked - batched seconds")
    plt.plot(x, y)
    plt.show()










# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

