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


def free_memory(collect: bool = True):
    """Return free memory pages to the OS.

    On Linux this calls glibc's malloc_trim(0).
    On macOS, malloc_zone_pressure_relief is used as the closest equivalent.

    collect: run a full gc.collect() first (the historical default). A full
    collection scans every live GC-tracked object in the process, so its cost
    grows with process age in long-lived workers that accumulate framework
    state (the 2026-08 Frontera drift investigation measured this). Callers
    invoking free_memory once per task should pass collect=False: task-local
    numpy buffers are freed by refcounting, and cyclic garbage is better
    handled by a process-level policy (gc.freeze at worker boot + raised
    thresholds).
    """
    if collect:
        gc.collect()
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
