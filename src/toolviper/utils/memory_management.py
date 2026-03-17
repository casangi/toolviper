



def get_rss_gb():
    import psutil, os
    return psutil.Process(os.getpid()).memory_info().rss / 1e9


def memory_setup(threshold: int=131072):
    """Set malloc mmap threshold to reduce heap fragmentation.

    On Linux this calls glibc's mallopt(M_MMAP_THRESHOLD, threshold).
    On macOS the system allocator handles this automatically; the call is skipped.
    """
    import sys, ctypes

    if sys.platform == "linux":
        ctypes.CDLL("libc.so.6").mallopt(-3, threshold)


def free_memory():
    """Return free memory pages to the OS.

    On Linux this calls glibc's malloc_trim(0).
    On macOS, malloc_zone_pressure_relief is used as the closest equivalent.
    """
    import sys, ctypes
    import gc
    gc.collect()
    if sys.platform == "linux":
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    elif sys.platform == "darwin":
        try:
            lib = ctypes.CDLL("libSystem.B.dylib")
            # malloc_zone_pressure_relief(zone=NULL, goal=0) asks all zones to
            # release as much memory as possible back to the OS.
            lib.malloc_zone_pressure_relief(None, 0)
        except Exception:
            pass
