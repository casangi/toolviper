import tracemalloc
import uuid
import csv
import functools
import multiprocessing
import time
import psutil

import toolviper.utils.logger as logger


def cpu_usage_(stop_event, filename):
    if filename is None:
        filename = f"cpu_usage_{uuid.uuid4()}.csv"

    with open(filename, "w") as csvfile:
        number_of_cores = psutil.cpu_count(logical=True)

        core_list = [f"c{core}" for core in range(number_of_cores)]
        writer = csv.writer(csvfile, delimiter=",", lineterminator="\n")
        writer.writerow(core_list)
        while not stop_event.is_set():
            usage = psutil.cpu_percent(percpu=True, interval=1)
            writer.writerow(usage)


def monitor(filename=None):
    def function_wrapper(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            stop_event = multiprocessing.Event()

            monitor_process = multiprocessing.Process(
                target=cpu_usage_, args=(stop_event, filename)
            )
            monitor_process.start()

            time.sleep(1)

            try:
                results = function(*args, **kwargs)
            finally:
                stop_event.set()
                monitor_process.join(timeout=1)
                monitor_process.terminate()

            return results

        return wrapper

    return function_wrapper


# Not for production. Yet.
def memory():
    def decorator(function):
        @functools.wraps(function)
        def wrapper(*args, **kwargs):
            import csv

            logger.debug(f"start memory profiling on function {function.__name__}")

            tracemalloc.start()
            result = function(*args, **kwargs)
            snapshot = tracemalloc.take_snapshot()
            snapshot = snapshot.filter_traces(
                (
                    tracemalloc.Filter(False, "<frozen importlib._bootstrap>"),
                    tracemalloc.Filter(False, "<unknown>"),
                )
            )
            stats = snapshot.statistics("lineno")
            record = []
            for index, stat in enumerate(stats, 1):
                frame = stat.traceback[0]
                record.append(
                    {
                        "index": index,
                        "filename": frame.filename,
                        "lineno": frame.lineno,
                        "memory": int(stat.size / 1024),
                    }
                )

            tracemalloc.stop()
            field_names = ["index", "filename", "lineno", "memory"]

            with open(
                f"memory_usage_{function.__name__}.csv",
                "w",
                newline="",
                encoding="utf-8",
            ) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_names)
                writer.writeheader()
                writer.writerows(record)

            return result

        return wrapper

    return decorator
