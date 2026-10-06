import asyncio
import csv
import os
import queue
import random
import ssl
import sys
import threading
import time
import tkinter as tk
from collections import Counter, defaultdict
from concurrent.futures import Future as ConcurrentFuture
from dataclasses import dataclass, field
from tkinter import filedialog, messagebox, ttk
from urllib.parse import quote as _quote, urljoin, urlparse, urlsplit, urlunsplit

import httpx

# Определение поддержки HTTP/2
try:
    import h2  # noqa: F401
    HTTP2_AVAILABLE = True
except ImportError:
    HTTP2_AVAILABLE = False


# ==============================================================================
# КОНСТАНТЫ И НАСТРОЙКИ
# ==============================================================================

MAX_URLS = 500
MAX_REDIRECTS = 15
ALLOWED_SCHEMES = ("http", "https")

GUI_POLL_INTERVAL_MS = 30
GUI_ROWS_PER_TICK = 15
TREE_INSERT_CHUNK = 100

REQUEST_METHODS = ("GET", "HEAD")
DEFAULT_METHOD = "GET"

HEAD_FALLBACK_STATUSES = (403, 405, 501)
REDIRECT_STATUSES = (301, 302, 303, 307, 308)
FALLBACK_ERROR_TYPES = ("connect_error", "ssl_error")

SAME_HOST_WARN_THRESHOLD = 50
CLOSE_GRACE_SECONDS = 3.0
DRAIN_FIRST_CHUNK_TIMEOUT = 0.5

_HAS_TASK_GROUP = sys.version_info >= (3, 11)

USER_AGENT_PRESETS = {
    "Яндекс.Бот (основной)": "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
    "Яндекс.Картинки": "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)",
    "Яндекс.Метрика": "Mozilla/5.0 (compatible; YandexMetrika/2.0; +http://yandex.com/bots)",
    "Яндекс.Мобильный": "Mozilla/5.0 (compatible; YandexMobileBot/3.0; +http://yandex.com/bots)",
    "Googlebot (desktop)": "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot (smartphone)": (
        "Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/W.X.Y.Z Mobile "
        "Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
    ),
    "Googlebot-Image": "Googlebot-Image/1.0",
    "Google-InspectionTool": "Mozilla/5.0 (compatible; Google-InspectionTool/1.0;)",
    "Bingbot": "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "DuckDuckBot": "DuckDuckBot/1.0; (+http://duckduckgo.com/duckduckbot.html)",
    "Chrome (Windows)": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Firefox (Windows)": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Свой User-Agent…": None,
}

DEFAULT_UA_PRESET = "Googlebot (desktop)"
BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru,en;q=0.9",
    "Range": "bytes=0-1024",
}


# ==============================================================================
# МОДЕЛИ ДАННЫХ
# ==============================================================================

@dataclass(slots=True)
class CheckConfig:
    delay: float
    connect_to: float
    read_to: float
    concurrency: int
    user_agent: str | None
    method: str
    http2: bool
    verify_ssl: bool

    @classmethod
    def from_ui(cls, raw: dict) -> "CheckConfig | None":
        try:
            delay = max(0.0, float(raw["delay"].replace(",", ".")))
            connect_to = float(raw["connect_to"].replace(",", "."))
            read_to = float(raw["read_to"].replace(",", "."))
            concurrency = int(raw["concurrency"])
        except (ValueError, TypeError, KeyError):
            return None

        connect_to = connect_to if connect_to > 0 else 5.0
        read_to = read_to if read_to > 0 else 10.0
        concurrency = max(1, min(100, concurrency))

        method = raw.get("method")
        if method not in REQUEST_METHODS:
            method = DEFAULT_METHOD

        http2 = bool(raw.get("http2")) and HTTP2_AVAILABLE
        verify_ssl = bool(raw.get("verify_ssl", False))

        return cls(
            delay=delay,
            connect_to=connect_to,
            read_to=read_to,
            concurrency=concurrency,
            user_agent=(raw.get("user_agent") or "").strip() or None,
            method=method,
            http2=http2,
            verify_ssl=verify_ssl,
        )


@dataclass(slots=True)
class HopInfo:
    url: str
    status: int | None
    elapsed: float


@dataclass(slots=True)
class CheckResult:
    index: int
    input_url: str
    status: int | None
    total_time: float
    final_url: str | None
    chain: list[HopInfo] = field(default_factory=list)
    error: str | None = None
    error_type: str | None = None


# ==============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ И УТИЛИТЫ
# ==============================================================================

def build_candidate_urls(raw: str) -> list[str]:
    u = raw.strip()
    if not u or u.startswith("#"):
        return []
    if u.startswith(("http://", "https://")):
        return [u]

    parsed = urlsplit("https://" + u)
    netloc = parsed.netloc
    path = parsed.path or ""
    query = parsed.query or ""
    fragment = parsed.fragment or ""

    if "@" in netloc:
        userinfo, _, host_port = netloc.rpartition("@")
        userinfo_prefix = userinfo + "@"
    else:
        userinfo_prefix = ""
        host_port = netloc

    is_ipv6_literal = host_port.startswith("[")
    if is_ipv6_literal:
        host = host_port
        port_part = ""
    elif ":" in host_port:
        host, _, port = host_port.rpartition(":")
        port_part = ":" + port
    else:
        host = host_port
        port_part = ""

    tail = path
    if query:
        tail += "?" + query
    if fragment:
        tail += "#" + fragment

    host_lower = host.lower()
    
    cands: list[str] = []
    
    # Исправленная подстановка портов
    if port_part in (":80", ":443"):
        https_port = ""
        http_port = ""
    else:
        https_port = port_part
        http_port = port_part

    if host_lower.startswith("www."):
        main_host = host
        alt_host = host[4:]
        cands.append(f"https://{userinfo_prefix}{main_host}{https_port}{tail}")
        cands.append(f"http://{userinfo_prefix}{main_host}{http_port}{tail}")
        cands.append(f"https://{userinfo_prefix}{alt_host}{https_port}{tail}")
        cands.append(f"http://{userinfo_prefix}{alt_host}{http_port}{tail}")
    else:
        cands.append(f"https://{userinfo_prefix}{host}{https_port}{tail}")
        cands.append(f"http://{userinfo_prefix}{host}{http_port}{tail}")
        if not is_ipv6_literal and host.count(".") == 1:
            www_host = "www." + host
            cands.append(f"https://{userinfo_prefix}{www_host}{https_port}{tail}")
            cands.append(f"http://{userinfo_prefix}{www_host}{http_port}{tail}")

    return cands


def is_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return bool(p.netloc and p.scheme in ALLOWED_SCHEMES)
    except Exception:
        return False


def dedup_key(url: str) -> str:
    try:
        p = urlparse(url)
        host = (p.netloc or "").lower()
        if host.startswith("www."):
            host = host[4:]
        if p.scheme == "http" and host.endswith(":80"):
            host = host[:-3]
        elif p.scheme == "https" and host.endswith(":443"):
            host = host[:-4]
        path = p.path or "/"
        return f"{host}{path}"
    except Exception:
        return url


def ensure_ascii_url(url: str) -> str:
    try:
        parts = urlsplit(url)
        if not parts.netloc:
            return url

        host, sep, port = parts.netloc.partition(":")
        if host and any(ord(c) > 127 for c in host):
            host = host.encode("idna").decode("ascii")
        netloc = host + (sep + port if sep else "")

        safe = "/%:=&?~#+!$,;'()*[]@"
        path = _quote(parts.path, safe=safe)
        query = _quote(parts.query, safe=safe)

        return urlunsplit((parts.scheme, netloc, path, query, parts.fragment))
    except (UnicodeError, ValueError):
        return url


def resolve_redirect(base: str, location: str) -> str:
    loc = (location or "").strip()
    if not loc:
        raise ValueError("Empty Location header")
    if loc.startswith("www.") and not loc.startswith(("www./", "www.?")):
        loc = "https://" + loc

    try:
        loc = _quote(loc, safe="/%:=&?~#+!$,;'()*[]@")
    except Exception:
        pass

    resolved = urljoin(base, loc)
    scheme = urlparse(resolved).scheme.lower()
    if scheme not in ALLOWED_SCHEMES:
        raise ValueError(f"Unsupported scheme '{scheme}'")
    if not urlparse(resolved).netloc:
        raise ValueError("Empty host after redirect resolution")
    return resolved


def csv_safe(value, sanitize: bool = True) -> str:
    s = "" if value is None else str(value)
    if sanitize and s and s[0] in ("=", "+", "-", "@", "\t", "\r", "\n"):
        return "'" + s
    return s


def format_chain(chain: list[HopInfo]) -> str:
    if not chain:
        return ""
    return "  →  ".join(
        f"{h.url} [{h.status if h.status is not None else 'ERR'}]" for h in chain
    )


def extract_ssl_cause(exc: BaseException) -> bool:
    seen: set[int] = set()
    cur: BaseException | None = exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        if isinstance(cur, ssl.SSLError):
            return True
        cur = cur.__cause__ or cur.__context__
    return False


# ==============================================================================
# АСИНХРОННЫЕ КОМПОНЕНТЫ ДВИЖКА (CORE ENGINE)
# ==============================================================================

class AsyncPerHostRateLimiter:
    def __init__(self, delay: float):
        self.delay = max(0.0, delay)
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._last_request_time: dict[str, float] = {}

    def reset(self) -> None:
        self._locks.clear()
        self._last_request_time.clear()

    async def wait(self, host: str) -> None:
        if self.delay <= 0:
            return

        lock = self._locks[host]
        async with lock:
            now = time.monotonic()
            last_time = self._last_request_time.get(host, 0.0)
            jitter = random.uniform(0.0, self.delay * 0.1)
            target_delay = self.delay + jitter
            
            elapsed = now - last_time
            if elapsed < target_delay:
                await asyncio.sleep(target_delay - elapsed)
            
            self._last_request_time[host] = time.monotonic()


class AsyncRunner:
    def __init__(self):
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._start()

    def _start(self):
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="asyncio-loop"
        )
        self._thread.start()
        if not self._ready.wait(timeout=5.0):
            raise RuntimeError("Asyncio event loop failed to start in 5 seconds")

    def _run_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        loop.call_soon(self._ready.set)
        try:
            loop.run_forever()
        finally:
            try:
                loop.run_until_complete(loop.shutdown_asyncgens())
            except Exception:
                pass
            try:
                loop.close()
            except Exception:
                pass

    def submit(self, coro) -> ConcurrentFuture:
        assert self._loop is not None
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def call_soon(self, fn, *args) -> None:
        if self._loop is None:
            return
        try:
            self._loop.call_soon_threadsafe(fn, *args)
        except Exception:
            pass

    def stop(self) -> None:
        loop = self._loop
        if loop is not None and loop.is_running():
            def _cancel_and_stop():
                for t in asyncio.all_tasks(loop):
                    t.cancel()
                loop.stop()
            try:
                loop.call_soon_threadsafe(_cancel_and_stop)
            except Exception:
                pass


async def _drain_first_chunk(resp: httpx.Response) -> None:
    try:
        iterator = resp.aiter_bytes(chunk_size=1024)
        await asyncio.wait_for(iterator.__anext__(), timeout=DRAIN_FIRST_CHUNK_TIMEOUT)
    except Exception:
        pass


async def _fetch_once(client: httpx.AsyncClient, method: str, url: str) -> dict:
    methods = [method]
    if method == "HEAD":
        methods.append("GET")

    last_error = None
    ascii_url = ensure_ascii_url(url)

    for m in methods:
        start = time.perf_counter()
        try:
            # ИСПРАВЛЕНИЕ: Использование client.stream вместо client.request(..., stream=True)
            async with client.stream(m, ascii_url) as resp:
                status = resp.status_code
                headers = dict(resp.headers)
                elapsed = time.perf_counter() - start

                if m == "GET":
                    await _drain_first_chunk(resp)

                if m == "HEAD" and status in HEAD_FALLBACK_STATUSES:
                    last_error = f"HEAD not supported (status {status})"
                    continue

                return {
                    "status": status,
                    "headers": headers,
                    "elapsed": elapsed,
                    "method": m,
                    "error": None,
                    "error_type": None,
                }
        except httpx.ConnectTimeout as e:
            return {
                "status": None,
                "headers": {},
                "elapsed": time.perf_counter() - start,
                "method": m,
                "error": f"ConnectTimeout: {e}"[:200],
                "error_type": "connect_error",
            }
        except httpx.TimeoutException as e:
            return {
                "status": None,
                "headers": {},
                "elapsed": time.perf_counter() - start,
                "method": m,
                "error": f"{type(e).__name__}: {e}"[:200],
                "error_type": "timeout",
            }
        except httpx.ConnectError as e:
            err_type = "ssl_error" if extract_ssl_cause(e) else "connect_error"
            return {
                "status": None,
                "headers": {},
                "elapsed": time.perf_counter() - start,
                "method": m,
                "error": f"{type(e).__name__}: {e}"[:200],
                "error_type": err_type,
            }
        except httpx.HTTPError as e:
            return {
                "status": None,
                "headers": {},
                "elapsed": time.perf_counter() - start,
                "method": m,
                "error": f"{type(e).__name__}: {e}"[:200],
                "error_type": "request_exc",
            }
        except Exception as e:
            return {
                "status": None,
                "headers": {},
                "elapsed": time.perf_counter() - start,
                "method": m,
                "error": f"{type(e).__name__}: {e}"[:200],
                "error_type": "other",
            }

    return {
        "status": None,
        "headers": {},
        "elapsed": 0.0,
        "method": method,
        "error": last_error or "HEAD fallback failed",
        "error_type": "head_fallback",
    }


def _next_hop(current_url: str, method: str, status: int, headers: dict) -> dict:
    if status not in REDIRECT_STATUSES:
        return {"action": "final", "final_status": status, "final_url": current_url}

    next_method = method
    if status in (301, 302, 303):
        if method not in ("GET", "HEAD"):
            next_method = "GET"

    location = headers.get("Location")
    if not location:
        return {"action": "final", "final_status": status, "final_url": current_url}

    try:
        next_url = resolve_redirect(current_url, location)
    except Exception as e:
        return {
            "action": "error",
            "error": f"Redirect resolve failed: {e}"[:200],
            "error_type": "bad_redirect",
        }

    return {"action": "redirect", "next_url": next_url, "next_method": next_method}


async def _check_chain(
    client: httpx.AsyncClient,
    start_url: str,
    index: int,
    limiter: AsyncPerHostRateLimiter,
    sem: asyncio.Semaphore,
    method: str,
) -> CheckResult:
    current = start_url
    chain: list[HopInfo] = []
    visited: set[str] = set()
    total_time = 0.0
    error = None
    error_type = None
    final_status = None
    final_url = None
    current_method = method

    for _ in range(MAX_REDIRECTS + 1):
        p = urlparse(ensure_ascii_url(current).lower())
        norm_current = f"{p.scheme}://{p.netloc}{p.path}"
        
        if norm_current in visited:
            error, error_type = "Redirect loop detected", "loop"
            chain.append(HopInfo(url=current, status=None, elapsed=0.0))
            break
        visited.add(norm_current)

        host = p.netloc
        await limiter.wait(host)

        async with sem:
            result = await _fetch_once(client, current_method, current)

        total_time += result["elapsed"]
        chain.append(HopInfo(url=current, status=result["status"], elapsed=result["elapsed"]))
        current_method = result["method"]

        if result["error"]:
            error, error_type = result["error"], result["error_type"]
            break

        hop = _next_hop(current, current_method, result["status"], result["headers"])
        if hop["action"] == "final":
            final_status, final_url = hop["final_status"], hop["final_url"]
            break
        if hop["action"] == "error":
            error, error_type = hop["error"], hop["error_type"]
            break
        current = hop["next_url"]
        current_method = hop["next_method"]
    else:
        error, error_type = "Too many redirects", "too_many_redirects"

    if final_status is None and error is None:
        error, error_type = "No response", "no_response"

    return CheckResult(
        index=index,
        input_url=start_url,
        status=final_status,
        total_time=total_time,
        final_url=final_url,
        chain=chain,
        error=error,
        error_type=error_type,
    )


async def check_url_candidates(
    client: httpx.AsyncClient,
    candidates: list[str],
    index: int,
    limiter: AsyncPerHostRateLimiter,
    sem: asyncio.Semaphore,
    method: str,
) -> CheckResult:
    last_res: CheckResult | None = None
    for i, url in enumerate(candidates):
        res = await _check_chain(client, url, index, limiter, sem, method)
        last_res = res
        if res.status is not None:
            res.input_url = candidates[0]
            return res
        is_last = i == len(candidates) - 1
        if is_last or res.error_type not in FALLBACK_ERROR_TYPES or len(res.chain) > 1:
            break

    if last_res is not None:
        last_res.input_url = candidates[0]
    return last_res


class URLCheckerEngine:
    def __init__(self, result_queue: queue.Queue):
        self.result_queue = result_queue
        self._runner = AsyncRunner()
        self._main_task: ConcurrentFuture | None = None
        self._asyncio_task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None
        self._running = threading.Event()
        self._stopped = threading.Event()

    def is_running(self) -> bool:
        return self._running.is_set()

    def start(self, candidates_per_index: list[list[str]], cfg: CheckConfig) -> None:
        if self.is_running():
            return
        self._running.set()
        self._stopped.clear()
        self._asyncio_task = None
        coro = self._run(candidates_per_index, cfg)
        self._main_task = self._runner.submit(coro)

    def request_stop(self) -> None:
        if not self.is_running():
            return

        task = self._asyncio_task
        if task is not None and not task.done():
            try:
                self._runner.call_soon(task.cancel)
                return
            except Exception:
                pass
        fut = self._main_task
        if fut is not None and not fut.done():
            try:
                self._runner.call_soon(fut.cancel)
            except Exception:
                pass

    def shutdown(self) -> None:
        self.request_stop()
        if self._main_task is not None:
            try:
                self._main_task.result(timeout=CLOSE_GRACE_SECONDS)
            except Exception:
                pass
        self._runner.stop()

    async def _run(self, candidates_per_index: list[list[str]], cfg: CheckConfig):
        self._asyncio_task = asyncio.current_task()

        headers = dict(BASE_HEADERS)
        if cfg.user_agent:
            headers["User-Agent"] = cfg.user_agent

        timeout = httpx.Timeout(
            connect=cfg.connect_to,
            read=cfg.read_to,
            write=cfg.read_to,
            pool=cfg.connect_to,
        )
        limits = httpx.Limits(
            max_connections=max(cfg.concurrency + 5, 10),
            max_keepalive_connections=cfg.concurrency,
        )

        limiter = AsyncPerHostRateLimiter(cfg.delay)
        limiter.reset()

        try:
            async with httpx.AsyncClient(
                headers=headers,
                timeout=timeout,
                limits=limits,
                follow_redirects=False,
                http2=cfg.http2,
                verify=cfg.verify_ssl,
            ) as client:
                self._client = client
                sem = asyncio.Semaphore(cfg.concurrency)

                async def task_wrapper(idx: int, cands: list[str]):
                    try:
                        res = await check_url_candidates(
                            client, cands, idx, limiter, sem, cfg.method
                        )
                        if res is not None:
                            self.result_queue.put(res)
                    except asyncio.CancelledError:
                        raise
                    except Exception as e:
                        self.result_queue.put(
                            CheckResult(
                                index=idx,
                                input_url=cands[0],
                                status=None,
                                total_time=0.0,
                                final_url=None,
                                error=f"{type(e).__name__}: {e}"[:200],
                                error_type="worker_exc",
                            )
                        )

                if _HAS_TASK_GROUP:
                    try:
                        async with asyncio.TaskGroup() as tg:
                            for i, cands in enumerate(candidates_per_index):
                                tg.create_task(task_wrapper(i, cands))
                    except* asyncio.CancelledError:
                        pass
                else:
                    tasks = [
                        asyncio.create_task(task_wrapper(i, cands))
                        for i, cands in enumerate(candidates_per_index)
                    ]
                    try:
                        await asyncio.gather(*tasks, return_exceptions=True)
                    except asyncio.CancelledError:
                        for t in tasks:
                            if not t.done():
                                t.cancel()
                        await asyncio.gather(*tasks, return_exceptions=True)
                        raise
        except asyncio.CancelledError:
            pass
        except Exception:
            pass
        finally:
            self._client = None
            self._asyncio_task = None
            self._running.clear()
            self._stopped.set()
            # Гарантированное информирование интерфейса о завершении
            self.result_queue.put("__DONE__")


# ==============================================================================
# ПОЛЬЗОВАТЕЛЬСКИЙ ИНТЕРФЕЙС (TKINTER GUI)
# ==============================================================================

class ChainDetailWindow(tk.Toplevel):
    def __init__(self, parent, result: CheckResult):
        super().__init__(parent)
        self.title("Детали проверки URL")
        self.geometry("920x500")
        self.minsize(700, 320)
        self.transient(parent)
        self.grab_set()

        top = ttk.Frame(self, padding=10)
        top.pack(fill="x")

        def add_row(r, label, value, color=None):
            ttk.Label(top, text=label, font=("", 9, "bold")).grid(row=r, column=0, sticky="w", pady=1)
            kw = {"font": ("Consolas", 9)}
            if color:
                kw["foreground"] = color
            ttk.Label(top, text=value, **kw).grid(row=r, column=1, sticky="w", pady=1)

        add_row(0, "Исходный URL:", result.input_url)
        add_row(1, "Финальный URL:", result.final_url or "—")
        add_row(2, "Итоговый статус:", str(result.status) if result.status is not None else "ERR")
        add_row(3, "Суммарное время:", f"{result.total_time:.3f} с")
        add_row(4, "Цепочка хопов:", str(len(result.chain)))
        if result.error:
            add_row(5, "Ошибка:", result.error, color="#c00000")
        top.columnconfigure(1, weight=1)

        mid = ttk.LabelFrame(self, text="Цепочка перенаправлений (Hops)")
        mid.pack(fill="both", expand=True, padx=10, pady=(4, 8))

        cols = ("step", "url", "status", "time")
        tree = ttk.Treeview(mid, columns=cols, show="headings")
        headings = [("step", "#", 45, "center"), ("url", "URL", 580, "w"),
                    ("status", "Статус", 90, "center"), ("time", "Время (с)", 100, "e")]
        for c, t, w, a in headings:
            tree.heading(c, text=t)
            tree.column(c, width=w, anchor=a, stretch=(c == "url"))

        vsb = ttk.Scrollbar(mid, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(mid, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        mid.rowconfigure(0, weight=1)
        mid.columnconfigure(0, weight=1)

        for i, hop in enumerate(result.chain, 1):
            st = str(hop.status) if hop.status is not None else "ERR"
            tree.insert("", "end", values=(i, hop.url, st, f"{hop.elapsed:.3f}"))

        bottom = ttk.Frame(self, padding=(10, 0, 10, 10))
        bottom.pack(fill="x")
        ttk.Button(bottom, text="Закрыть", command=self.destroy).pack(side="right")
        self.bind("<Escape>", lambda e: self.destroy())


class ExportOptionsDialog(tk.Toplevel):
    DELIMITERS = [
        (";   точка с запятой (Excel RU)", ";"),
        (",   запятая", ","),
        ("\\t  табуляция", "\t"),
    ]
    ENCODINGS = [
        ("UTF-8 с BOM (Excel)", "utf-8-sig"),
        ("UTF-8 без BOM", "utf-8"),
        ("Windows-1251 (старый Excel RU)", "cp1251"),
    ]

    def __init__(self, parent, default_delim: str, default_enc: str, default_errors_only: bool = False, default_sanitize: bool = True):
        super().__init__(parent)
        self.title("Параметры экспорта CSV")
        self.resizable(False, False)
        self.transient(parent)
        self.grab_set()
        self.result: tuple[str, str, bool, bool] | None = None

        frm = ttk.Frame(self, padding=12)
        frm.pack(fill="both", expand=True)

        ttk.Label(frm, text="Разделитель:").grid(row=0, column=0, sticky="w", pady=4)
        self.delim_var = tk.StringVar(value=self._label_for(self.DELIMITERS, default_delim))
        ttk.Combobox(
            frm, textvariable=self.delim_var, values=[l for l, _ in self.DELIMITERS], state="readonly", width=34
        ).grid(row=0, column=1, sticky="ew", pady=4, padx=(8, 0))

        ttk.Label(frm, text="Кодировка:").grid(row=1, column=0, sticky="w", pady=4)
        self.enc_var = tk.StringVar(value=self._label_for(self.ENCODINGS, default_enc))
        ttk.Combobox(
            frm, textvariable=self.enc_var, values=[l for l, _ in self.ENCODINGS], state="readonly", width=34
        ).grid(row=1, column=1, sticky="ew", pady=4, padx=(8, 0))

        self.errors_only_var = tk.BooleanVar(value=default_errors_only)
        ttk.Checkbutton(frm, text="Только ошибки (4xx/5xx/ERR)", variable=self.errors_only_var).grid(
            row=2, column=0, columnspan=2, sticky="w", pady=(4, 0)
        )

        self.sanitize_var = tk.BooleanVar(value=default_sanitize)
        ttk.Checkbutton(frm, text="Защита от CSV-инъекций (экранировать =, +, -, @)", variable=self.sanitize_var).grid(
            row=3, column=0, columnspan=2, sticky="w", pady=(4, 0)
        )

        btns = ttk.Frame(frm)
        btns.grid(row=4, column=0, columnspan=2, sticky="e", pady=(12, 0))
        ttk.Button(btns, text="OK", command=self._ok).pack(side="right", padx=(6, 0))
        ttk.Button(btns, text="Отмена", command=self.destroy).pack(side="right")

        frm.columnconfigure(1, weight=1)
        self.bind("<Return>", lambda e: self._ok())
        self.bind("<Escape>", lambda e: self.destroy())

        self.update_idletasks()
        self._center_on(parent)

    def _center_on(self, parent):
        try:
            px, py = parent.winfo_rootx(), parent.winfo_rooty()
            pw, ph = parent.winfo_width(), parent.winfo_height()
            w, h = self.winfo_width(), self.winfo_height()
            x = px + max(0, (pw - w) // 2)
            y = py + max(0, (ph - h) // 3)
            self.geometry(f"+{x}+{y}")
        except Exception:
            pass

    @staticmethod
    def _label_for(options, value):
        for label, v in options:
            if v == value:
                return label
        return options[0][0]

    @staticmethod
    def _value_for(options, label):
        for l, v in options:
            if l == label:
                return v
        return options[0][1]

    def _ok(self):
        self.result = (
            self._value_for(self.DELIMITERS, self.delim_var.get()),
            self._value_for(self.ENCODINGS, self.enc_var.get()),
            bool(self.errors_only_var.get()),
            bool(self.sanitize_var.get()),
        )
        self.destroy()


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("HTTP Status & Redirect Checker (Windows Pro)")
        self.geometry("1360x900")
        self.minsize(1080, 660)

        self.result_queue: queue.Queue = queue.Queue()
        self.engine = URLCheckerEngine(self.result_queue)

        self.results: dict[int, CheckResult] = {}
        self.total_urls = 0
        self.done_count = 0
        self._iid_by_index: dict[int, str] = {}
        self._index_by_iid: dict[str, int] = {}
        self._urls: list[str] = []
        self._candidates: list[list[str]] = []
        self._config: CheckConfig | None = None
        self._done_seen = False
        self._finalized = False
        self._closing = False

        self._pending_inserts: list[tuple[int, str]] = []

        self._export_delim = ";"
        self._export_encoding = "utf-8-sig"
        self._export_errors_only = False
        self._export_sanitize = True

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        main = ttk.Frame(self, padding=8)
        main.pack(fill="both", expand=True)

        top = ttk.LabelFrame(main, text="Список URL (до 500 штук, по одному в строке)")
        top.pack(fill="both", expand=False)

        self.input_text = tk.Text(top, height=6, wrap="none", font=("Consolas", 9))
        self.input_text.pack(fill="both", expand=True, padx=4, pady=4)

        ua_frame = ttk.LabelFrame(main, text="User-Agent")
        ua_frame.pack(fill="x", pady=(6, 0))

        row1 = ttk.Frame(ua_frame, padding=(6, 4))
        row1.pack(fill="x")

        ttk.Label(row1, text="Пресет:").pack(side="left")
        self.ua_preset_var = tk.StringVar(value=DEFAULT_UA_PRESET)
        self.ua_combo = ttk.Combobox(
            row1, textvariable=self.ua_preset_var, values=list(USER_AGENT_PRESETS.keys()), state="readonly", width=26
        )
        self.ua_combo.pack(side="left", padx=(4, 14))
        self.ua_combo.bind("<<ComboboxSelected>>", self._on_ua_preset_change)

        ttk.Label(row1, text="Свой User-Agent:").pack(side="left")
        self.ua_custom_var = tk.StringVar()
        self.ua_entry = ttk.Entry(row1, textvariable=self.ua_custom_var)
        self.ua_entry.pack(side="left", fill="x", expand=True, padx=(4, 0))
        self.ua_entry.bind("<KeyRelease>", self._on_ua_entry_type)

        row2 = ttk.Frame(ua_frame, padding=(6, 0, 6, 6))
        row2.pack(fill="x")
        self.ua_preview_var = tk.StringVar()
        ttk.Label(row2, textvariable=self.ua_preview_var, foreground="#555", font=("Consolas", 8)).pack(side="left")

        self._on_ua_preset_change()

        opts = ttk.Frame(main)
        opts.pack(fill="x", pady=(6, 3))

        for label, var, default in (
            ("Задержка/хост (с):", "delay_var", "0.3"),
            ("Connect Timeout (с):", "connect_to_var", "5"),
            ("Read Timeout (с):", "read_to_var", "10"),
            ("Параллельно:", "concurrency_var", "20"),
        ):
            ttk.Label(opts, text=label).pack(side="left")
            v = tk.StringVar(value=default)
            setattr(self, var, v)
            ttk.Entry(opts, textvariable=v, width=5).pack(side="left", padx=(4, 10))

        ttk.Label(opts, text="Метод:").pack(side="left")
        self.method_var = tk.StringVar(value=DEFAULT_METHOD)
        ttk.Combobox(opts, textvariable=self.method_var, values=list(REQUEST_METHODS), state="readonly", width=6).pack(
            side="left", padx=(4, 10)
        )

        self.dedup_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(opts, text="Дедупликация", variable=self.dedup_var).pack(side="left", padx=(4, 10))

        self.http2_var = tk.BooleanVar(value=False)
        self.http2_check = ttk.Checkbutton(
            opts, text="HTTP/2", variable=self.http2_var, state="normal" if HTTP2_AVAILABLE else "disabled"
        )
        self.http2_check.pack(side="left", padx=(4, 10))

        self.ignore_ssl_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(opts, text="Игнорировать ошибки SSL", variable=self.ignore_ssl_var).pack(side="left", padx=(4, 10))

        actions = ttk.Frame(main)
        actions.pack(fill="x", pady=(0, 6))

        self.start_btn = ttk.Button(actions, text="Запустить", command=self.on_start)
        self.start_btn.pack(side="left", padx=3)
        self.stop_btn = ttk.Button(actions, text="Стоп", command=self.on_stop, state="disabled")
        self.stop_btn.pack(side="left", padx=3)
        self.clear_btn = ttk.Button(actions, text="Очистить", command=self.on_clear)
        self.clear_btn.pack(side="left", padx=3)
        self.export_btn = ttk.Button(actions, text="Экспорт в CSV", command=self.on_export)
        self.export_btn.pack(side="left", padx=3)
        ttk.Label(actions, text="(Двойной клик по строке открывает подробности)", foreground="#666").pack(
            side="left", padx=12
        )

        prog_frame = ttk.Frame(main)
        prog_frame.pack(fill="x", pady=(0, 6))
        self.progress = ttk.Progressbar(prog_frame, mode="determinate")
        self.progress.pack(side="left", fill="x", expand=True)
        self.progress_label = ttk.Label(prog_frame, text="0 / 0")
        self.progress_label.pack(side="left", padx=8)

        results_frame = ttk.LabelFrame(main, text="Результаты сканирования")
        results_frame.pack(fill="both", expand=True)

        columns = ("idx", "input", "status", "time", "hops", "final", "chain", "error")
        self.tree = ttk.Treeview(results_frame, columns=columns, show="headings")
        headings = {
            "idx": "#",
            "input": "Исходный URL",
            "status": "Код",
            "time": "Время (с)",
            "hops": "Хопов",
            "final": "Финальный URL",
            "chain": "Цепочка редиректов",
            "error": "Ошибка",
        }
        widths = {"idx": 45, "input": 260, "status": 55, "time": 75, "hops": 55, "final": 260, "chain": 380, "error": 220}
        for c in columns:
            self.tree.heading(c, text=headings[c])
            self.tree.column(c, width=widths[c], anchor="w", stretch=False)

        vsb = ttk.Scrollbar(results_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(results_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        results_frame.rowconfigure(0, weight=1)
        results_frame.columnconfigure(0, weight=1)

        for tag, bg in (("ok", "#e7f7e7"), ("redir", "#fff8d6"), ("warn", "#ffe8cc"), ("err", "#ffdede")):
            self.tree.tag_configure(tag, background=bg)
        self.tree.tag_configure("pending", foreground="#999999")
        self.tree.bind("<Double-1>", self._on_tree_double_click)

        self.status_var = tk.StringVar(value="Готов к работе")
        ttk.Label(main, textvariable=self.status_var, anchor="w", relief="sunken").pack(
            fill="x", side="bottom", pady=(6, 0)
        )

    def _on_ua_preset_change(self, event=None):
        preset = self.ua_preset_var.get()
        ua = USER_AGENT_PRESETS.get(preset)
        if ua is None:
            self.ua_preview_var.set("Введите свой User-Agent выше ↑")
            self.after_idle(self.ua_entry.focus_set)
        else:
            self.ua_custom_var.set(ua)
            self.ua_preview_var.set(f"→ {ua}")

    def _on_ua_entry_type(self, event=None):
        if self.ua_preset_var.get() != "Свой User-Agent…":
            self.ua_preset_var.set("Свой User-Agent…")
        self.ua_preview_var.set(f"→ {self.ua_custom_var.get().strip() or '(пусто)'}")

    def _get_user_agent(self) -> str | None:
        return self.ua_custom_var.get().strip() or None

    def _on_tree_double_click(self, event):
        iid = self.tree.identify_row(event.y)
        if not iid:
            return
        idx = self._index_by_iid.get(iid)
        if idx is None:
            return
        res = self.results.get(idx)
        if res is None:
            return
        try:
            ChainDetailWindow(self, res)
        except Exception as e:
            messagebox.showerror("Ошибка окна", str(e))

    def on_start(self):
        if self.engine.is_running():
            messagebox.showinfo("Идёт работа", "Дождитесь завершения текущей проверки.")
            return

        raw_lines = self.input_text.get("1.0", "end").splitlines()
        seen: set[str] = set()
        candidates_per_index: list[list[str]] = []
        dup_removed = 0
        invalid = 0

        for line in raw_lines:
            cands = build_candidate_urls(line)
            if not cands:
                continue
            if not is_valid_url(cands[0]):
                invalid += 1
                continue
            if self.dedup_var.get():
                key = dedup_key(cands[0])
                if key in seen:
                    dup_removed += 1
                    continue
                seen.add(key)
            candidates_per_index.append(cands)

        if not candidates_per_index:
            messagebox.showwarning(
                "Нет URL",
                "Не найдено ни одного валидного URL для проверки."
                + (f"\nОтброшено невалидных: {invalid}" if invalid else ""),
            )
            return

        truncated = 0
        if len(candidates_per_index) > MAX_URLS:
            truncated = len(candidates_per_index) - MAX_URLS
            candidates_per_index = candidates_per_index[:MAX_URLS]

        cfg = CheckConfig.from_ui(
            {
                "delay": self.delay_var.get(),
                "connect_to": self.connect_to_var.get(),
                "read_to": self.read_to_var.get(),
                "concurrency": self.concurrency_var.get(),
                "user_agent": self._get_user_agent(),
                "method": self.method_var.get(),
                "http2": self.http2_var.get(),
                "verify_ssl": not self.ignore_ssl_var.get(),
            }
        )
        if cfg is None:
            messagebox.showerror("Ошибка параметров", "Проверьте числовые поля ввода.")
            return

        hosts = Counter(urlparse(c[0]).netloc for c in candidates_per_index)
        if hosts and cfg.delay > 0:
            top_host, top_count = hosts.most_common(1)[0]
            if top_count >= SAME_HOST_WARN_THRESHOLD:
                est = top_count * cfg.delay
                if not messagebox.askyesno(
                    "Внимание: Нагрузка на один хост",
                    f"Хост «{top_host}» встречается {top_count} раз.\n"
                    f"При задержке {cfg.delay} с проверка займет не менее "
                    f"{est:.0f} с (~{est / 60:.1f} мин).\n\nПродолжить?",
                ):
                    return

        self._pending_inserts.clear()
        self.tree.delete(*self.tree.get_children())
        self.results.clear()
        self._iid_by_index.clear()
        self._index_by_iid.clear()
        self._urls = [c[0] for c in candidates_per_index]
        self._candidates = candidates_per_index
        self._config = cfg
        self.total_urls = len(self._urls)
        self.done_count = 0
        self._done_seen = False
        self._finalized = False

        self.progress["maximum"] = self.total_urls
        self.progress["value"] = 0
        self.progress_label.config(text=f"0 / {self.total_urls}")

        notes = []
        if dup_removed:
            notes.append(f"дубликатов: {dup_removed}")
        if invalid:
            notes.append(f"невалидных: {invalid}")
        if truncated:
            notes.append(f"обрезано до {MAX_URLS}: {truncated}")
        note_str = (" (" + ", ".join(notes) + ")") if notes else ""

        self.status_var.set(f"Подготовка строк...{note_str}")
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.clear_btn.config(state="disabled")

        self._pending_inserts = list(enumerate(self._urls))
        self.after(1, self._populate_tree_chunk)

    def _populate_tree_chunk(self):
        if self._closing or not self._pending_inserts:
            return
        chunk = self._pending_inserts[:TREE_INSERT_CHUNK]
        self._pending_inserts = self._pending_inserts[TREE_INSERT_CHUNK:]
        for i, u in chunk:
            iid = self.tree.insert("", "end", values=(i + 1, u, "…", "…", "", "", "", ""), tags=("pending",))
            self._iid_by_index[i] = iid
            self._index_by_iid[iid] = i

        if self._pending_inserts:
            self.after(1, self._populate_tree_chunk)
        else:
            if self._candidates and self._config:
                self.status_var.set(f"Выполнение… Метод: {self._config.method}")
                self.engine.start(self._candidates, self._config)
                self.after(GUI_POLL_INTERVAL_MS, self._poll_results)

    def _poll_results(self):
        if self._closing:
            return

        drained = 0
        while drained < GUI_ROWS_PER_TICK:
            try:
                item = self.result_queue.get_nowait()
            except queue.Empty:
                break

            if item == "__DONE__":
                self._done_seen = True
                continue

            if isinstance(item, CheckResult):
                self.results[item.index] = item
                self.done_count += 1

                iid = self._iid_by_index.get(item.index)
                if iid and self.tree.exists(iid):
                    st = str(item.status) if item.status is not None else "ERR"
                    hops_cnt = str(len(item.chain)) if item.chain else "0"
                    final_u = item.final_url or "—"
                    chain_str = format_chain(item.chain)
                    err_str = item.error or ""

                    tag = "err"
                    if item.status is not None:
                        if 200 <= item.status < 300:
                            tag = "ok"
                        elif 300 <= item.status < 400:
                            tag = "redir"
                        elif 400 <= item.status < 500:
                            tag = "warn"

                    self.tree.item(
                        iid,
                        values=(
                            item.index + 1,
                            item.input_url,
                            st,
                            f"{item.total_time:.3f}",
                            hops_cnt,
                            final_u,
                            chain_str,
                            err_str,
                        ),
                        tags=(tag,),
                    )

                self.progress["value"] = self.done_count
                self.progress_label.config(text=f"{self.done_count} / {self.total_urls}")
            drained += 1

        if self._done_seen and not self._finalized:
            self._finalize_run()

        if self.engine.is_running() or not self._done_seen:
            self.after(GUI_POLL_INTERVAL_MS, self._poll_results)

    def _finalize_run(self):
        self._finalized = True
        self.start_btn.config(state="normal")
        self.stop_btn.config(state="disabled")
        self.clear_btn.config(state="normal")
        self.status_var.set(f"Завершено. Проверено {self.done_count} из {self.total_urls} URL.")

    def on_stop(self):
        if not self.engine.is_running():
            return
        self.engine.request_stop()
        self.stop_btn.config(state="disabled")
        self.status_var.set("Остановка по требованию...")

    def on_clear(self):
        if self.engine.is_running():
            return
        self._pending_inserts.clear()
        self.input_text.delete("1.0", "end")
        self.tree.delete(*self.tree.get_children())
        self.results.clear()
        self._iid_by_index.clear()
        self._index_by_iid.clear()
        self._urls = []
        self._candidates = []
        self._config = None
        self.done_count = 0
        self.total_urls = 0
        self._done_seen = False
        self._finalized = False
        self.progress["value"] = 0
        self.progress_label.config(text="0 / 0")
        self.status_var.set("Готов к работе")

    def on_export(self):
        if not self.results:
            messagebox.showinfo("Экспорт невозможно выполнить", "Результаты отсутствуют.")
            return

        dlg = ExportOptionsDialog(
            self, self._export_delim, self._export_encoding, self._export_errors_only, self._export_sanitize
        )
        self.wait_window(dlg)
        if dlg.result is None:
            return

        self._export_delim, self._export_encoding, self._export_errors_only, self._export_sanitize = dlg.result

        filepath = filedialog.asksaveasfilename(
            parent=self,
            title="Сохранить экспорт CSV",
            defaultextension=".csv",
            filetypes=[("CSV файлы", "*.csv"), ("Все файлы", "*.*")],
        )
        if not filepath:
            return

        try:
            with open(filepath, "w", newline="", encoding=self._export_encoding) as f:
                writer = csv.writer(f, delimiter=self._export_delim)
                writer.writerow([
                    "#", "Исходный URL", "Финальный статус", "Общее время (с)", 
                    "Количество хопов", "Финальный URL", "Цепочка редиректов", "Ошибка"
                ])

                for idx in sorted(self.results.keys()):
                    res = self.results[idx]
                    
                    if self._export_errors_only:
                        is_error = res.error is not None or (res.status is not None and res.status >= 400)
                        if not is_error:
                            continue

                    st = str(res.status) if res.status is not None else "ERR"
                    chain_str = format_chain(res.chain)
                    
                    writer.writerow([
                        res.index + 1,
                        csv_safe(res.input_url, self._export_sanitize),
                        st,
                        f"{res.total_time:.3f}",
                        len(res.chain),
                        csv_safe(res.final_url or "", self._export_sanitize),
                        csv_safe(chain_str, self._export_sanitize),
                        csv_safe(res.error or "", self._export_sanitize),
                    ])

            messagebox.showinfo("Успех", f"Данные успешно экспортированы в:\n{filepath}")
        except Exception as e:
            messagebox.showerror("Ошибка экспорта", f"Не удалось сохранить файл:\n{e}")

    def _on_close(self):
        self._closing = True
        if self.engine.is_running():
            self.engine.shutdown()
        self.destroy()


# ==============================================================================
# ТОЧКА ВХОДА
# ==============================================================================

if __name__ == "__main__":
    app = App()
    app.mainloop()
