import ctypes
import gc
import os
import sys

_libc = None
_mmap_threshold = None


def get_rss_gb():
    import psutil

    return psutil.Process(os.getpid()).memory_info().rss / 1e9


def _get_libc():
    global _libc
    if _libc is None:
        _libc = ctypes.CDLL("libc.so.6")
    return _libc


def memory_setup(threshold: int = 131072):
    """Set malloc mmap threshold to reduce heap fragmentation.

    On Linux this calls glibc's mallopt(M_MMAP_THRESHOLD, threshold).
    On macOS the system allocator handles this automatically; the call is skipped.

    Idempotent per process: mallopt permanently disables glibc's dynamic
    threshold adaptation, so repeated calls with the same value are skipped.
    If the process was started with the MALLOC_MMAP_THRESHOLD_ environment
    variable set, glibc applied the policy at startup and this function defers
    to it entirely -- setting the policy once in the worker/rank environment is
    preferred over per-task calls.
    """
    global _mmap_threshold
    if sys.platform != "linux":
        return
    if "MALLOC_MMAP_THRESHOLD_" in os.environ:
        return
    if _mmap_threshold == threshold:
        return
    _get_libc().mallopt(-3, threshold)  # -3 = M_MMAP_THRESHOLD
    _mmap_threshold = threshold


def free_memory(collect: bool = True, trim: bool = True):
    """Return free memory pages to the OS.

    On Linux this calls glibc's malloc_trim(0).
    On macOS, malloc_zone_pressure_relief is used as the closest equivalent.

    collect: run a full gc.collect() first (the historical default). Keep it
    on for tasks whose xarray/pandas objects form reference cycles holding
    large numpy buffers -- refcounting never frees cycles (disabling this
    OOMed the 2026-08-10 Frontera run). Pair with gc.freeze at worker boot to
    keep the per-task collection cheap.

    trim: release freed allocator memory back to the OS (the historical
    default). Passing trim=False keeps freed heap for reuse, so repeated
    same-size buffers stop being re-faulted from fresh zero pages each task --
    the 2026-08-11 Frontera diagnosis found that re-fault churn (~29 GB/task)
    is what makes tasks slow down as node memory fragments and transparent
    huge pages collapse to 4 KiB. Only combine trim=False with a high
    MALLOC_MMAP_THRESHOLD_/MALLOC_TRIM_THRESHOLD_ environment and watch RSS:
    it trades eager release for a stable resident working set.
    """
    if collect:
        gc.collect()
    if not trim:
        return
    if sys.platform == "linux":
        _get_libc().malloc_trim(0)
    elif sys.platform == "darwin":
        try:
            lib = ctypes.CDLL("libSystem.B.dylib")
            # malloc_zone_pressure_relief(zone=NULL, goal=0) asks all zones to
            # release as much memory as possible back to the OS.
            lib.malloc_zone_pressure_relief(None, 0)
        except Exception:
            pass
