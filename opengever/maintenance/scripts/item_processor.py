from datetime import datetime
from ZODB.POSException import ConflictError
import time
import transaction

"""
This script provides a conflict safe item processor with time estimation and
the overall progress.

if not running the processor with dry_run, the transaction will be commited
automatically between batches. The default batch-size is 200 items.
"""

class Processor:
    """Conflict save item processing with time estimation

    Usage:

def printer(item):
    print item

    Processor().run([1,2,3,4], process_item_method=printer, dry_run=True)
    """
    process_item_method = None

    def run(self, items, batch_size=200, dry_run=False,
            process_item_method=lambda item: item,
            batch_committed_method=None):
        self.process_item_method = process_item_method
        self.batch_committed_method = batch_committed_method

        self.do_for_items(items, batch_size, dry_run)

        if dry_run:
            transaction.abort()
        else:
            transaction.commit()

        print "Done!"

    def process_batch(self, items):
        for item in items:
            self.process_item_method(item)

    def do_for_items(self, items, batch_size=200, dry_run=False):
        tt = TimeTracker(len(items), b_size=batch_size).reset()

        for current_batch_count, batched_items in enumerate(Batch(items, batch_size=batch_size)):
            has_conflict_error = True
            max_retry_on_conflict = 3
            conflict_retries = 0

            while has_conflict_error :
                has_conflict_error = False
                try:
                    self.process_batch(batched_items)

                    if not dry_run:
                        transaction.commit()

                    tt.print_log(current_batch_count * batch_size + len(batched_items))

                    if self.batch_committed_method:
                        self.batch_committed_method()

                except ConflictError as error:
                    conflict_retries += 1
                    has_conflict_error = True
                    transaction.abort()

                    if conflict_retries > max_retry_on_conflict:
                        print "Could not resolve conflict error in batch {}".format(
                            current_batch_count)
                        raise error

                    print "ConflictError in batch {}. Retry #{}".format(
                        current_batch_count, conflict_retries)


class TimeTracker(object):
    """Responsible for statistics and printing it to the console.

    Usage:
    tt = TimeTracker(len(brains))
    for i, c in enumerate(brains):
    tt.maybe_print(i)

    """
    def __init__(self, total, b_size=100, statistic_x_batch_items=20):
        self.total = total
        self.b_size = b_size
        self.statistic_x_batch_items = statistic_x_batch_items
        self.last_x_batch_times = []
        self.start_time = None

    def reset(self):
        self.last_x_batch_times = []
        self.start_time = time.time()
        return self

    def maybe_print(self, current_count):
        if current_count % self.b_size == 0:
            self.print_log(current_count)
            return True
        return False

    def print_log(self, current_count):
        try:
            if len(self.last_x_batch_times) > self.statistic_x_batch_items:
                del self.last_x_batch_times[0]
            self.last_x_batch_times.append(time.time() - self.start_time)
            remaining_time = sum(self.last_x_batch_times) / len(self.last_x_batch_times) / self.b_size * (self.total - current_count) / 60
        except:
            remaining_time = 0

        self.start_time = time.time()

        ts = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        if remaining_time > 0:
            print("{} Processed {}/{} objs. remaining estimated time: {} min".format(
                ts, current_count, self.total, int(remaining_time)))
        else:
            print("{} Processed {}/{} objs. remaining estimated time: {} sec".format(
                ts, current_count, self.total, int(remaining_time * 60)))

        self.print_progress(current_count)

    def print_progress(self, current_count):
        number_done = int (float(current_count) / self.total * 100)
        number_remeaining = 100 - number_done
        print "[{}{}] {}%".format('#' * number_done, ' ' * number_remeaining, number_done)


class Batch:
    """Responsible for batching.

    Usage:
    batch = Batch([1,2,3,4], 2)
    [i for i in batch]
    [[1,2], [2,3]
    """
    def __init__(self, data, batch_size):
        self.data = data
        self.batch_size = batch_size

    def __len__(self):
        return len(self.data)

    def __getitem__(self, index):
        return self.data[index]

    def __iter__(self):
        self.current_index = 0
        return self

    def __next__(self):
        return self.next()

    def next(self):
        if self.current_index >= len(self):
            raise StopIteration
        batch_data = self.data[self.current_index:self.current_index+self.batch_size]
        self.current_index += self.batch_size
        return batch_data
