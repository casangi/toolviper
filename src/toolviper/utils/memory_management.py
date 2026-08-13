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


_MALLINFO_FIELDS = ("arena", "ordblks", "smblks", "hblks", "hblkhd",
                    "usmblks", "fsmblks", "uordblks", "fordblks", "keepcost")


class _MallInfo2(ctypes.Structure):
    _fields_ = [(n, ctypes.c_size_t) for n in _MALLINFO_FIELDS]


class _MallInfo(ctypes.Structure):  # legacy int fields (glibc < 2.33)
    _fields_ = [(n, ctypes.c_int) for n in _MALLINFO_FIELDS]


def _mallinfo_str() -> str:
    """glibc heap statistics as a compact string ('' off-Linux / on failure).

    Prefers mallinfo2 (glibc >= 2.33, size_t fields); falls back to the
    legacy mallinfo, whose int fields wrap above 2 GiB (still indicative --
    Frontera's CentOS 7 glibc 2.17 only has the legacy call)."""
    if sys.platform != "linux":
        return ""
    try:
        libc = _get_libc()
        try:
            fn = libc.mallinfo2
            fn.restype = _MallInfo2
        except AttributeError:
            fn = libc.mallinfo
            fn.restype = _MallInfo
        mi = fn()
        gb = 1e9
        return (f"heap_sbrk={mi.arena / gb:.2f}GB "
                f"heap_used={mi.uordblks / gb:.2f}GB "
                f"heap_free={mi.fordblks / gb:.2f}GB "
                f"mmap={mi.hblkhd / gb:.2f}GB")
    except Exception:  # noqa: BLE001 -- best-effort diagnostics, never raise
        return ""


def memory_setup_summary() -> str:
    """One-line description of the allocator setup this process runs under:
    the malloc/graphviper environment knobs that are set, plus any mmap
    threshold pinned by memory_setup()."""
    keys = ("MALLOC_TRIM_THRESHOLD_", "MALLOC_MMAP_THRESHOLD_",
            "MALLOC_ARENA_MAX", "MALLOC_CONF",
            "GRAPHVIPER_TASK_MEMORY_MANAGEMENT",
            "GRAPHVIPER_PER_TASK_GC", "GRAPHVIPER_PER_TASK_TRIM",
            "GRAPHVIPER_LOG_MEMORY_STATE")
    env = {k: os.environ[k] for k in keys if k in os.environ}
    return f"env={env} mallopt_mmap_threshold={_mmap_threshold}"


def memory_state_summary() -> str:
    """Compact one-line snapshot of the process memory state, for per-task
    logging (the 2026-08 Frontera fragmentation investigation): current RSS,
    VMA count, cumulative minor faults, GC counters, and glibc heap stats
    when available. Linux-focused; degrades gracefully elsewhere."""
    import resource

    parts = []
    if sys.platform == "linux":
        try:
            with open("/proc/self/statm") as f:
                rss_pages = int(f.read().split()[1])
            parts.append(f"rss={rss_pages * os.sysconf('SC_PAGE_SIZE') / 1e9:.2f}GB")
        except OSError:
            pass
        try:
            with open("/proc/self/maps") as f:
                parts.append(f"vma={sum(1 for _ in f)}")
        except OSError:
            pass
    ru = resource.getrusage(resource.RUSAGE_SELF)
    if sys.platform == "darwin":  # ru_maxrss is bytes on macOS, KiB on Linux
        parts.append(f"peak_rss={ru.ru_maxrss / 1e9:.2f}GB")
    else:
        parts.append(f"peak_rss={ru.ru_maxrss * 1024 / 1e9:.2f}GB")
    parts.append(f"minflt={ru.ru_minflt}")
    parts.append(f"gc_count={gc.get_count()}")
    parts.append(f"gc_frozen={gc.get_freeze_count()}")
    mi = _mallinfo_str()
    if mi:
        parts.append(mi)
    return " ".join(parts)


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
