import csv
import os
import queue
import socket as _socket
import threading
import time
import tkinter as tk
import weakref
from collections import Counter
from concurrent.futures import (
    CancelledError, ThreadPoolExecutor,
    wait as futures_wait, FIRST_COMPLETED,
)
from tkinter import ttk, filedialog, messagebox
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import (
    ConnectionError as ReqConnectionError,
    ConnectTimeout,
    RequestException,
    Timeout,
)
from requests.utils import requote_uri
from urllib3.connection import HTTPConnection, HTTPSConnection
from urllib3.connectionpool import HTTPConnectionPool, HTTPSConnectionPool
from urllib3.poolmanager import PoolManager

# ---------- Пресеты User-Agent ----------
USER_AGENT_PRESETS = {
    "Яндекс.Бот (основной)":
        "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
    "Яндекс.Картинки":
        "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)",
    "Яндекс.Метрика":
        "Mozilla/5.0 (compatible; YandexMetrika/2.0; +http://yandex.com/bots)",
    "Яндекс.Мобильный":
        "Mozilla/5.0 (compatible; YandexMobileBot/3.0; +http://yandex.com/bots)",

    "Googlebot (desktop)":
        "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Googlebot (smartphone)":
        ("Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) "
         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/W.X.Y.Z Mobile "
         "Safari/537.36 (compatible; Googlebot/2.1; "
         "+http://www.google.com/bot.html)"),
    "Googlebot-Image":
        "Googlebot-Image/1.0",
    "Google-InspectionTool":
        "Mozilla/5.0 (compatible; Google-InspectionTool/1.0;)",

    "Bingbot":
        "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Bingbot (mobile)":
        ("Mozilla/5.0 (Linux; Android 6.0.1; Nexus 5X Build/MMB29P) "
         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/W.X.Y.Z Mobile "
         "Safari/537.36 (compatible; bingbot/2.0; "
         "+http://www.bing.com/bingbot.htm)"),
    "MSN/Bing Preview":
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/534+ (KHTML, like Gecko) BingPreview/1.0b",

    "DuckDuckBot":
        "DuckDuckBot/1.0; (+http://duckduckgo.com/duckduckbot.html)",
    "Baidu Spider":
        "Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)",
    "Applebot":
        "Mozilla/5.0 (compatible; Applebot/0.1; +http://www.apple.com/go/applebot)",

    "Chrome (Windows)":
        ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
         "AppleWebKit/537.36 (KHTML, like Gecko) "
         "Chrome/122.0 Safari/537.36"),
    "Firefox (Windows)":
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Safari (macOS)":
        ("Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) "
         "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"),

    "Свой User-Agent…": None,
}

DEFAULT_UA_PRESET = "Googlebot (desktop)"

BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru,en;q=0.9",
}

MAX_URLS = 500
MAX_REDIRECTS = 15
ALLOWED_SCHEMES = ("http", "https")

GUI_ROWS_PER_TICK = 50
GUI_POLL_INTERVAL_MS = 40

REQUEST_METHODS = ("GET", "HEAD")
DEFAULT_METHOD = "GET"
HEAD_FALLBACK_STATUSES = (405, 501)
REDIRECT_STATUSES = (301, 302, 303, 307, 308)
METHOD_SWITCHING_STATUSES = (301, 302, 303)

SAME_HOST_WARN_THRESHOLD = 50

# Сколько ждём мягкого завершения воркера перед жёстким os._exit
CLOSE_GRACE_SECONDS = 1.5


# ---------- Socket registry ----------
class SocketRegistry:
    """Реестр активных сокетов urllib3.

    `shutdown_all()` вызывает `sock.shutdown(SHUT_RDWR)` — это
    единственный способ прервать блокирующий `recv()` в рабочем
    потоке. `session.close()` и `adapter.close()` для этого НЕ годятся:
    они очищают только idle-очередь пула (`poolmanager.clear()`),
    а активные соединения, взятые через `_get_conn()`, остаются
    в `_ConnectionHolder._in_use` и не трогаются.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._socks: weakref.WeakSet = weakref.WeakSet()

    def register(self, sock):
        if sock is None:
            return
        with self._lock:
            self._socks.add(sock)

    def shutdown_all(self):
        with self._lock:
            socks = list(self._socks)
            self._socks.clear()
        for s in socks:
            try:
                s.shutdown(_socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                s.close()
            except Exception:
                pass

    def clear(self):
        with self._lock:
            self._socks.clear()


_SOCKET_REGISTRY = SocketRegistry()


# ---------- Tracked connections / pools / adapter ----------
class _TrackedHTTPConnection(HTTPConnection):
    def connect(self):
        super().connect()
        # Best-effort: если urllib3 сменит имя атрибута, просто теряем
        # возможность обрыва, но запросы продолжают работать.
        try:
            _SOCKET_REGISTRY.register(getattr(self, "sock", None))
        except Exception:
            pass


class _TrackedHTTPSConnection(HTTPSConnection):
    def connect(self):
        super().connect()
        try:
            _SOCKET_REGISTRY.register(getattr(self, "sock", None))
        except Exception:
            pass


class _TrackedHTTPConnectionPool(HTTPConnectionPool):
    ConnectionCls = _TrackedHTTPConnection


class _TrackedHTTPSConnectionPool(HTTPSConnectionPool):
    ConnectionCls = _TrackedHTTPSConnection


class TrackedHTTPAdapter(HTTPAdapter):
    """HTTPAdapter с пулами, регистрирующими сокеты.

    Глобальный urllib3 не трогаем — только собственный PoolManager
    в рамках этой сессии.
    """

    def init_poolmanager(self, connections, maxsize, block=False, **kw):
        self.poolmanager = PoolManager(
            num_pools=connections, maxsize=maxsize,
            block=block, **kw,
        )
        self.poolmanager.pool_classes_by_scheme = {
            "http": _TrackedHTTPConnectionPool,
            "https": _TrackedHTTPSConnectionPool,
        }


# ---------- Per-host Rate Limiter ----------
class PerHostRateLimiter:
    def __init__(self, delay: float, stop_event: threading.Event):
        self.delay = max(0.0, delay)
        self.stop_event = stop_event
        self._lock = threading.Lock()
        self._next_slot: dict[str, float] = {}

    def wait(self, host: str) -> bool:
        if self.delay <= 0:
            return not self.stop_event.is_set()
        with self._lock:
            now = time.monotonic()
            slot = max(now, self._next_slot.get(host, 0.0))
            self._next_slot[host] = slot + self.delay
        wait_time = slot - time.monotonic()
        if wait_time > 0 and self.stop_event.wait(wait_time):
            return False
        return not self.stop_event.is_set()

    def reset(self):
        with self._lock:
            self._next_slot.clear()


# ---------- Пул HTTP-сессий ----------
class SessionPool:
    def __init__(self, user_agent: str | None):
        self.user_agent = user_agent
        self._local = threading.local()
        self._all: list[requests.Session] = []
        self._lock = threading.Lock()
        self._closed = False

    def get(self) -> requests.Session | None:
        with self._lock:
            if self._closed:
                return None
            s: requests.Session | None = getattr(self._local, "session", None)
            if s is not None:
                return s
            s = requests.Session()
            s.headers.update(BASE_HEADERS)
            if self.user_agent:
                s.headers["User-Agent"] = self.user_agent
            adapter = TrackedHTTPAdapter(
                pool_connections=2, pool_maxsize=2, max_retries=0,
            )
            s.mount("http://", adapter)
            s.mount("https://", adapter)
            self._local.session = s
            self._all.append(s)
            return s

    def close_all(self):
        with self._lock:
            if self._closed:
                return
            self._closed = True
            sessions = list(self._all)
            self._all.clear()
        for s in sessions:
            try:
                s.close()
            except Exception:
                pass


# ---------- Утилиты ----------
def build_candidate_urls(raw: str) -> list[str]:
    u = raw.strip()
    if not u or u.startswith("#"):
        return []
    if u.startswith(("http://", "https://")):
        return [u]
    return ["https://" + u, "http://" + u]


def is_valid_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return bool(p.netloc and p.scheme in ALLOWED_SCHEMES)
    except Exception:
        return False


def deduplicate(urls: list[str]) -> list[str]:
    return list(dict.fromkeys(urls))


def ensure_ascii_url(url: str) -> str:
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return url
        host, sep, port = parsed.netloc.partition(":")
        if not host:
            return url
        if any(ord(c) > 127 for c in host):
            host = host.encode("idna").decode("ascii")
        netloc = host + (sep + port if sep else "")
        return urlunparse(parsed._replace(netloc=netloc))
    except (UnicodeError, ValueError):
        return url


def resolve_redirect(base: str, location: str) -> str:
    loc = (location or "").strip()
    if not loc:
        raise ValueError("empty Location")
    if loc.startswith("www.") and not loc.startswith(("www./", "www.?")):
        loc = "https://" + loc
    try:
        loc = requote_uri(loc)
    except Exception:
        pass
    resolved = urljoin(base, loc)
    scheme = urlparse(resolved).scheme.lower()
    if scheme not in ALLOWED_SCHEMES:
        raise ValueError(f"unsupported scheme '{scheme}'")
    if not urlparse(resolved).netloc:
        raise ValueError("empty host after resolve")
    return resolved


# ---------- Один HTTP-запрос ----------
def _fetch_once(session, method, url, timeout_tuple):
    """Один HTTP-запрос с fallback HEAD→GET на 405/501.

    Порядок except:
      - ConnectTimeout — подкласс и Timeout, и ConnectionError.
        Идёт первым, чтобы пометить как `connect_error` (разрешает
        fallback https → http).
      - Timeout — покрывает ReadTimeout.
      - ReqConnectionError — refused/reset.
      - UnicodeEncodeError — не подкласс RequestException, но важен
        для IDN в URL.
      - RequestException — общий родитель.
    """
    methods = [method]
    if method == "HEAD":
        methods.append("GET")

    last_error = None
    for m in methods:
        start = time.perf_counter()
        try:
            with session.request(
                m, url, timeout=timeout_tuple,
                allow_redirects=False, stream=True,
            ) as r:
                status = r.status_code
                headers = r.headers
                elapsed = time.perf_counter() - start
                if m == "HEAD" and status in HEAD_FALLBACK_STATUSES:
                    last_error = f"HEAD not supported (status {status})"
                    continue
                return {
                    "status": status, "headers": headers,
                    "elapsed": elapsed, "method": m,
                    "error": None, "error_type": None,
                }
        except ConnectTimeout as e:
            return {
                "status": None, "headers": {},
                "elapsed": time.perf_counter() - start, "method": m,
                "error": f"ConnectTimeout: {e}"[:200],
                "error_type": "connect_error",
            }
        except Timeout as e:
            return {
                "status": None, "headers": {},
                "elapsed": time.perf_counter() - start, "method": m,
                "error": f"Timeout: {e}"[:200],
                "error_type": "timeout",
            }
        except ReqConnectionError as e:
            return {
                "status": None, "headers": {},
                "elapsed": time.perf_counter() - start, "method": m,
                "error": f"ConnectionError: {e}"[:200],
                "error_type": "connect_error",
            }
        except UnicodeEncodeError as e:
            return {
                "status": None, "headers": {},
                "elapsed": time.perf_counter() - start, "method": m,
                "error": f"UnicodeEncodeError: {e}"[:200],
                "error_type": "unicode",
            }
        except RequestException as e:
            return {
                "status": None, "headers": {},
                "elapsed": time.perf_counter() - start, "method": m,
                "error": f"{type(e).__name__}: {e}"[:200],
                "error_type": "request_exc",
            }
        except Exception as e:
            return {
                "status": None, "headers": {},
                "elapsed": time.perf_counter() - start, "method": m,
                "error": f"{type(e).__name__}: {e}"[:200],
                "error_type": "other",
            }

    return {
        "status": None, "headers": {}, "elapsed": 0.0, "method": method,
        "error": last_error or "HEAD fallback failed",
        "error_type": "head_fallback",
    }


def _is_connection_error(res) -> bool:
    if res.get("chain"):
        return False
    return res.get("error_type") == "connect_error"


# ---------- Проверка цепочки и URL ----------
def _check_chain(start_url, index, timeout_tuple, limiter,
                 stop_event, session_pool, method):
    session = session_pool.get()
    if session is None:
        return None
    session.cookies.clear()

    current = ensure_ascii_url(start_url)
    chain: list[tuple[str, int | None, float]] = []
    visited: set[str] = set()
    total_time = 0.0
    error = None
    error_type = None
    final_status = None
    final_url = None
    current_method = method

    for _ in range(MAX_REDIRECTS + 1):
        if stop_event.is_set():
            return None
        if current in visited:
            error, error_type = "Redirect loop detected", "loop"
            break
        visited.add(current)

        host = urlparse(current).netloc
        if not limiter.wait(host):
            return None
        if stop_event.is_set():
            return None

        result = _fetch_once(session, current_method, current, timeout_tuple)
        total_time += result["elapsed"]
        chain.append((current, result["status"], result["elapsed"]))
        current_method = result["method"]

        if result["error"]:
            if stop_event.is_set():
                return None
            error, error_type = result["error"], result["error_type"]
            break

        status = result["status"]
        if status in REDIRECT_STATUSES:
            location = result["headers"].get("Location")
            if status in METHOD_SWITCHING_STATUSES and current_method != "GET":
                current_method = "GET"
            if not location:
                final_status, final_url = status, current
                break
            try:
                next_url = resolve_redirect(current, location)
            except (ValueError, UnicodeError) as e:
                error = f"Invalid redirect Location: {e}"[:200]
                error_type = "bad_redirect"
                break
            except Exception as e:
                error = f"Redirect resolve failed: {e}"[:200]
                error_type = "bad_redirect"
                break
            current = next_url
            continue
        else:
            final_status, final_url = status, current
            break
    else:
        error, error_type = "Too many redirects", "too_many_redirects"

    if final_status is None and error is None:
        error, error_type = "No response", "no_response"

    return {
        "index": index, "input": start_url,
        "status": final_status, "time": total_time,
        "final": final_url, "chain": chain,
        "error": error, "error_type": error_type,
    }


def check_url(candidates, index, timeout_tuple, limiter,
              stop_event, session_pool, method):
    last_result = None
    for i, url in enumerate(candidates):
        if stop_event.is_set():
            return None
        res = _check_chain(url, index, timeout_tuple, limiter,
                           stop_event, session_pool, method)
        if res is None:
            return None
        last_result = res
        if res["status"] is not None:
            res["input"] = candidates[0]
            return res
        is_last = (i == len(candidates) - 1)
        if is_last or not _is_connection_error(res):
            break

    if last_result is not None:
        last_result["input"] = candidates[0]
    return last_result


def format_chain(chain):
    if not chain:
        return ""
    return "  →  ".join(
        f"{u} [{s if s is not None else 'ERR'}]" for u, s, _t in chain
    )


# ---------- Движок ----------
class URLCheckerEngine:
    """Изолированный движок. Общается с UI только через
    `result_queue` и `stop_event`. Никаких ссылок на Tk-объекты.
    """

    def __init__(self, result_queue: queue.Queue,
                 stop_event: threading.Event):
        self.result_queue = result_queue
        self.stop_event = stop_event
        self._pool: SessionPool | None = None
        self._worker: threading.Thread | None = None

    def is_running(self) -> bool:
        return self._worker is not None and self._worker.is_alive()

    def start(self, candidates_per_index, delay, timeout_tuple,
              workers, user_agent, method):
        if self.is_running():
            return
        self._worker = threading.Thread(
            target=self._run,
            args=(candidates_per_index, delay, timeout_tuple,
                  workers, user_agent, method),
            daemon=True,
        )
        self._worker.start()

    def request_stop(self):
        self.stop_event.set()
        try:
            _SOCKET_REGISTRY.shutdown_all()
        except Exception:
            pass
        pool = self._pool
        if pool is not None:
            try:
                pool.close_all()
            except Exception:
                pass

    def _run(self, candidates_per_index, delay, timeout_tuple,
             workers, user_agent, method):
        limiter = PerHostRateLimiter(delay, self.stop_event)
        pool = SessionPool(user_agent)
        self._pool = pool
        ex = ThreadPoolExecutor(max_workers=workers)

        def task(idx, cands):
            if self.stop_event.is_set():
                return None
            return check_url(cands, idx, timeout_tuple,
                             limiter, self.stop_event, pool, method)

        future_to_idx = {
            ex.submit(task, i, cands): i
            for i, cands in enumerate(candidates_per_index)
        }
        pending = set(future_to_idx.keys())
        stop_handled = False

        # Ручной цикл вместо as_completed: нужна возможность отменить
        # и прервать цикл по stop_event каждые 200 мс. as_completed
        # блокируется до следующего результата и не даёт этого.
        # add_done_callback вызывает колбэк в воркер-потоке, что тоже
        # не решает задачу прерывания цикла из GUI-потока.
        try:
            while pending:
                if self.stop_event.is_set() and not stop_handled:
                    stop_handled = True
                    for f in pending:
                        f.cancel()
                    pool.close_all()
                    _SOCKET_REGISTRY.shutdown_all()

                done, pending = futures_wait(
                    pending, timeout=0.2, return_when=FIRST_COMPLETED,
                )

                for f in done:
                    idx = future_to_idx[f]
                    try:
                        res = f.result()
                    except CancelledError:
                        continue
                    except Exception as e:
                        res = {
                            "index": idx,
                            "input": candidates_per_index[idx][0],
                            "status": None, "time": 0.0,
                            "final": None, "chain": [],
                            "error": f"{type(e).__name__}: {e}"[:200],
                            "error_type": "worker_exc",
                        }
                    if res is not None:
                        self.result_queue.put(res)
        finally:
            try:
                pool.close_all()
            except Exception:
                pass
            try:
                limiter.reset()
            except Exception:
                pass

            # wait=True: _worker не вернётся, пока все воркеры не
            # завершатся. Это позволяет watchdog'у в _on_close
            # проверить "жив ли worker" и принять решение без os._exit.
            ex.shutdown(wait=True, cancel_futures=True)
            self._pool = None
            self.result_queue.put("__DONE__")


# ---------- Диалог деталей ----------
class ChainDetailWindow(tk.Toplevel):
    def __init__(self, parent, result: dict):
        super().__init__(parent)
        self.title("Детали проверки")
        self.geometry("900x480")
        self.minsize(700, 320)
        self.transient(parent)
        self.grab_set()

        top = ttk.Frame(self, padding=8)
        top.pack(fill="x")

        def add_row(r, label, value, color=None):
            ttk.Label(top, text=label,
                      font=("", 9, "bold")).grid(row=r, column=0, sticky="w")
            kw = {"font": ("Consolas", 9)}
            if color:
                kw["foreground"] = color
            ttk.Label(top, text=value, **kw).grid(row=r, column=1, sticky="w")

        add_row(0, "Исходный URL:", result.get("input", ""))
        add_row(1, "Финальный URL:", result.get("final") or "—")
        status = result.get("status")
        add_row(2, "Итоговый статус:",
                str(status) if status is not None else "ERR")
        add_row(3, "Суммарное TTFB:", f"{result.get('time', 0.0):.3f} с")
        add_row(4, "Хопов:", str(len(result.get("chain", []))))
        if result.get("error"):
            add_row(5, "Ошибка:", result["error"], color="#a00")
        top.columnconfigure(1, weight=1)

        mid = ttk.LabelFrame(self, text="Шаги")
        mid.pack(fill="both", expand=True, padx=8, pady=(4, 8))

        cols = ("step", "url", "status", "time")
        tree = ttk.Treeview(mid, columns=cols, show="headings")
        for c, t, w, a in (
            ("step", "#", 45, "center"),
            ("url", "URL", 560, "w"),
            ("status", "Статус", 90, "center"),
            ("time", "TTFB, с", 100, "e"),
        ):
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

        for i, (url, st, dt) in enumerate(result.get("chain", []), 1):
            tree.insert("", "end", values=(
                i, url, str(st) if st is not None else "ERR", f"{dt:.3f}",
            ))

        bottom = ttk.Frame(self, padding=(8, 0, 8, 8))
        bottom.pack(fill="x")
        ttk.Button(bottom, text="Закрыть",
                   command=self.destroy).pack(side="right")
        self.bind("<Escape>", lambda e: self.destroy())


# ---------- GUI ----------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("HTTP Status Checker")
        self.geometry("1360x900")
        self.minsize(1080, 660)

        self.stop_event = threading.Event()
        self.result_queue: queue.Queue = queue.Queue()
        self.engine = URLCheckerEngine(self.result_queue, self.stop_event)

        # results: dict[int, dict] по index — O(1) доступ для двойного
        # клика и экспорта. Порядок восстанавливается сортировкой по ключу.
        self.results: dict[int, dict] = {}
        self.total_urls = 0
        self.done_count = 0
        self._iid_by_index: dict[int, str] = {}
        self._urls: list[str] = []
        self._done_seen = False

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_ui(self):
        main = ttk.Frame(self, padding=8)
        main.pack(fill="both", expand=True)

        top = ttk.LabelFrame(
            main, text="URLs (по одному в строке, до 500; схема опциональна)")
        top.pack(fill="both", expand=False)

        self.input_text = tk.Text(top, height=6, wrap="none",
                                  font=("Consolas", 9))
        self.input_text.pack(fill="both", expand=True, padx=4, pady=4)

        # --- User-Agent ---
        ua_frame = ttk.LabelFrame(main, text="User-Agent")
        ua_frame.pack(fill="x", pady=(6, 0))

        row1 = ttk.Frame(ua_frame, padding=(6, 4))
        row1.pack(fill="x")

        ttk.Label(row1, text="Пресет:").pack(side="left")
        self.ua_preset_var = tk.StringVar(value=DEFAULT_UA_PRESET)
        self.ua_combo = ttk.Combobox(
            row1, textvariable=self.ua_preset_var,
            values=list(USER_AGENT_PRESETS.keys()),
            state="readonly", width=26,
        )
        self.ua_combo.pack(side="left", padx=(4, 14))
        self.ua_combo.bind("<<ComboboxSelected>>", self._on_ua_preset_change)

        ttk.Label(row1, text="Свой UA:").pack(side="left")
        self.ua_custom_var = tk.StringVar()
        self.ua_entry = ttk.Entry(row1, textvariable=self.ua_custom_var)
        self.ua_entry.pack(side="left", fill="x", expand=True, padx=(4, 0))
        self.ua_entry.bind("<Button-1>", self._on_ua_entry_click)
        self.ua_entry.bind("<KeyRelease>", self._on_ua_entry_type)

        row2 = ttk.Frame(ua_frame, padding=(6, 0, 6, 6))
        row2.pack(fill="x")
        self.ua_preview_var = tk.StringVar()
        ttk.Label(row2, textvariable=self.ua_preview_var,
                  foreground="#555", font=("Consolas", 8)).pack(side="left")

        self._on_ua_preset_change()

        # --- Опции ---
        opts = ttk.Frame(main)
        opts.pack(fill="x", pady=(6, 3))

        for label, var, default in (
            ("Задержка/хост (с):", "delay_var", "0.3"),
            ("Connect (с):", "connect_to_var", "5"),
            ("Read (с):", "read_to_var", "10"),
            ("Потоков:", "threads_var", "5"),
        ):
            ttk.Label(opts, text=label).pack(side="left")
            v = tk.StringVar(value=default)
            setattr(self, var, v)
            ttk.Entry(opts, textvariable=v, width=5).pack(
                side="left", padx=(4, 10))

        ttk.Label(opts, text="Метод:").pack(side="left")
        self.method_var = tk.StringVar(value=DEFAULT_METHOD)
        ttk.Combobox(opts, textvariable=self.method_var,
                     values=list(REQUEST_METHODS), state="readonly",
                     width=6).pack(side="left", padx=(4, 10))

        self.dedup_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(opts, text="Дедуп.",
                        variable=self.dedup_var).pack(side="left", padx=(4, 10))

        actions = ttk.Frame(main)
        actions.pack(fill="x", pady=(0, 6))

        self.start_btn = ttk.Button(actions, text="Запустить",
                                    command=self.on_start)
        self.start_btn.pack(side="left", padx=3)
        self.stop_btn = ttk.Button(actions, text="Стоп", command=self.on_stop,
                                   state="disabled")
        self.stop_btn.pack(side="left", padx=3)
        self.clear_btn = ttk.Button(actions, text="Очистить",
                                    command=self.on_clear)
        self.clear_btn.pack(side="left", padx=3)
        self.export_btn = ttk.Button(actions, text="Экспорт CSV",
                                     command=self.on_export)
        self.export_btn.pack(side="left", padx=3)
        ttk.Label(actions, text="(двойной клик по строке — детали)",
                  foreground="#666").pack(side="left", padx=12)

        prog_frame = ttk.Frame(main)
        prog_frame.pack(fill="x", pady=(0, 6))
        self.progress = ttk.Progressbar(prog_frame, mode="determinate")
        self.progress.pack(side="left", fill="x", expand=True)
        self.progress_label = ttk.Label(prog_frame, text="0 / 0")
        self.progress_label.pack(side="left", padx=8)

        results_frame = ttk.LabelFrame(main, text="Результаты")
        results_frame.pack(fill="both", expand=True)

        columns = ("idx", "input", "status", "time", "hops",
                   "final", "chain", "error")
        self.tree = ttk.Treeview(results_frame, columns=columns, show="headings")
        headings = {
            "idx": "#", "input": "Исходный URL", "status": "Код",
            "time": "TTFB, с", "hops": "Хопов",
            "final": "Финальный URL", "chain": "Цепочка редиректов",
            "error": "Ошибка",
        }
        widths = {"idx": 45, "input": 260, "status": 55, "time": 75,
                  "hops": 55, "final": 260, "chain": 380, "error": 220}
        for c in columns:
            self.tree.heading(c, text=headings[c])
            self.tree.column(c, width=widths[c], anchor="w", stretch=False)

        vsb = ttk.Scrollbar(results_frame, orient="vertical",
                            command=self.tree.yview)
        hsb = ttk.Scrollbar(results_frame, orient="horizontal",
                            command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        results_frame.rowconfigure(0, weight=1)
        results_frame.columnconfigure(0, weight=1)

        for tag, bg in (("ok", "#e7f7e7"), ("redir", "#fff8d6"),
                        ("warn", "#ffe8cc"), ("err", "#ffdede")):
            self.tree.tag_configure(tag, background=bg)
        self.tree.tag_configure("pending", foreground="#999999")
        self.tree.bind("<Double-1>", self._on_tree_double_click)

        self.status_var = tk.StringVar(value="Готов")
        ttk.Label(main, textvariable=self.status_var, anchor="w",
                  relief="sunken").pack(fill="x", side="bottom", pady=(6, 0))

    # ---------- User-Agent ----------
    def _on_ua_preset_change(self, event=None):
        preset = self.ua_preset_var.get()
        ua = USER_AGENT_PRESETS.get(preset)
        if ua is None:
            self.ua_entry.config(state="normal")
            self.ua_preview_var.set("Введите свой User-Agent ↑")
            self.after_idle(self.ua_entry.focus_set)
        else:
            self.ua_custom_var.set(ua)
            self.ua_entry.config(state="readonly")
            self.ua_preview_var.set(
                f"→ {ua}    (кликните по полю, чтобы изменить)")

    def _on_ua_entry_click(self, event=None):
        if self.ua_preset_var.get() != "Свой User-Agent…":
            self.ua_preset_var.set("Свой User-Agent…")
            self._on_ua_preset_change()
            self.after_idle(lambda: self.ua_entry.icursor("end"))

    def _on_ua_entry_type(self, event=None):
        self.ua_preview_var.set(
            f"→ {self.ua_custom_var.get().strip() or '(пусто)'}")

    def _get_user_agent(self):
        return self.ua_custom_var.get().strip() or None

    def _on_tree_double_click(self, event):
        iid = self.tree.identify_row(event.y)
        if not iid:
            return
        # O(1) поиск: _iid_by_index даёт нам index по iid, results — dict.
        # Находим index по iid через обратный словарь (строим один раз).
        idx = None
        for k, v in self._iid_by_index.items():
            if v == iid:
                idx = k
                break
        if idx is None:
            return
        result = self.results.get(idx)
        if result is None:
            return
        try:
            ChainDetailWindow(self, result)
        except Exception as e:
            messagebox.showerror("Ошибка окна", str(e))

    # ---------- Кнопки ----------
    def on_start(self):
        if self.engine.is_running():
            messagebox.showinfo("Идёт работа",
                                "Дождитесь завершения текущей проверки.")
            return

        _SOCKET_REGISTRY.clear()

        raw_lines = self.input_text.get("1.0", "end").splitlines()
        seen: set[str] = set()
        candidates_per_index: list[list[str]] = []
        dup_removed = 0
        invalid = 0

        for line in raw_lines:
            cands = build_candidate_urls(line)
            if not cands:
                continue
            key = cands[0]
            if not is_valid_url(key):
                invalid += 1
                continue
            if self.dedup_var.get():
                if key in seen:
                    dup_removed += 1
                    continue
                seen.add(key)
            candidates_per_index.append(cands)

        if not candidates_per_index:
            messagebox.showwarning(
                "Нет URL",
                "Не найдено ни одного валидного URL для проверки."
                + (f"\nОтброшено невалидных: {invalid}" if invalid else ""))
            return

        truncated = 0
        if len(candidates_per_index) > MAX_URLS:
            truncated = len(candidates_per_index) - MAX_URLS
            candidates_per_index = candidates_per_index[:MAX_URLS]

        try:
            delay = float(self.delay_var.get().replace(",", "."))
            connect_to = float(self.connect_to_var.get().replace(",", "."))
            read_to = float(self.read_to_var.get().replace(",", "."))
            workers = int(self.threads_var.get())
        except ValueError:
            messagebox.showerror("Ошибка параметров",
                                 "Проверьте числовые поля.")
            return

        delay = max(0.0, delay)
        connect_to = connect_to if connect_to > 0 else 5.0
        read_to = read_to if read_to > 0 else 10.0
        timeout_tuple = (connect_to, read_to)
        workers = max(1, min(50, workers))
        method = (self.method_var.get()
                  if self.method_var.get() in REQUEST_METHODS
                  else DEFAULT_METHOD)
        user_agent = self._get_user_agent()

        if not user_agent and not messagebox.askyesno(
            "Пустой User-Agent",
            "Поле User-Agent пустое. Продолжить без заголовка User-Agent?"
        ):
            return

        hosts = Counter(urlparse(c[0]).netloc for c in candidates_per_index)
        if hosts and delay > 0:
            top_host, top_count = hosts.most_common(1)[0]
            if top_count >= SAME_HOST_WARN_THRESHOLD:
                est = top_count * delay
                if not messagebox.askyesno(
                    "Много URL на один хост",
                    f"Хост «{top_host}» встречается {top_count} раз.\n"
                    f"При задержке {delay} с это займёт минимум "
                    f"{est:.0f} с (~{est / 60:.1f} мин) только для этого "
                    f"хоста.\n\nПродолжить?"
                ):
                    return

        # Сброс состояния
        self.tree.delete(*self.tree.get_children())
        self.results.clear()
        self._iid_by_index.clear()
        self._urls = [c[0] for c in candidates_per_index]
        self.total_urls = len(self._urls)
        self.done_count = 0
        self._done_seen = False
        self.stop_event.clear()

        for i, u in enumerate(self._urls):
            iid = self.tree.insert("", "end", values=(
                i + 1, u, "…", "…", "", "", "", "",
            ), tags=("pending",))
            self._iid_by_index[i] = iid

        self.progress["maximum"] = self.total_urls
        self.progress["value"] = 0
        self.progress_label.config(text=f"0 / {self.total_urls}")

        notes = []
        if dup_removed:
            notes.append(f"дубликатов убрано: {dup_removed}")
        if invalid:
            notes.append(f"невалидных отброшено: {invalid}")
        if truncated:
            notes.append(f"обрезано до {MAX_URLS}: {truncated}")
        note_str = (" (" + ", ".join(notes) + ")") if notes else ""

        self.status_var.set(
            f"Работа…{note_str} метод: {method}, UA: "
            f"{(user_agent or '(нет)')[:60]}"
        )
        self.start_btn.config(state="disabled")
        self.stop_btn.config(state="normal")
        self.clear_btn.config(state="disabled")

        self.engine.start(candidates_per_index, delay, timeout_tuple,
                          workers, user_agent, method)
        self.after(GUI_POLL_INTERVAL_MS, self._poll_results)

    def on_stop(self):
        if self.stop_event.is_set():
            return
        self.engine.request_stop()
        self.status_var.set("Остановка… (прерывание активных сокетов)")

    def on_clear(self):
        if self.engine.is_running():
            return
        self.input_text.delete("1.0", "end")
        self.tree.delete(*self.tree.get_children())
        self.results.clear()
        self._iid_by_index.clear()
        self._urls = []
        self.done_count = 0
        self.total_urls = 0
        self._done_seen = False
        self.progress["value"] = 0
        self.progress_label.config(text="0 / 0")
        self.status_var.set("Готов")

    def on_export(self):
        if not self.results:
            messagebox.showinfo("Нет данных", "Нечего экспортировать.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv"), ("All files", "*.*")],
            title="Сохранить результаты",
        )
        if not path:
            return
        try:
            ordered = [self.results[k] for k in sorted(self.results)]
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
                w.writerow(["#", "Исходный URL", "Код", "TTFB (с)",
                            "Хопов", "Финальный URL", "Цепочка", "Ошибка"])
                for i, r in enumerate(ordered, 1):
                    w.writerow([
                        i, r["input"],
                        r["status"] if r["status"] is not None else "",
                        f"{r['time']:.3f}" if r["time"] else "",
                        len(r.get("chain", [])),
                        r["final"] or "",
                        format_chain(r["chain"]),
                        r["error"] or "",
                    ])
            self.status_var.set(f"Экспортировано: {path}")
        except Exception as e:
            messagebox.showerror("Ошибка экспорта", str(e))

    # ---------- Закрытие окна ----------
    def _on_close(self):
        """Порядок:
        1. stop_event + shutdown сокетов → блокирующие recv() падают.
        2. destroy() UI — mainloop выходит немедленно.
        3. Watchdog: ждём мягкого завершения воркера.

        Про `os._exit`. Воркеры `ThreadPoolExecutor` в Py3.9+ —
        НЕ-daemon и зарегистрированы в `threading._register_atexit`
        через `concurrent.futures.thread._python_exit`. При выходе
        интерпретатор вызовет `_python_exit`, который блокируется на
        `t.join()` для каждого воркера. Наш `_worker` — daemon, но
        воркеры executor'а внутри него — нет, поэтому `daemon=True`
        на `_worker` не спасает.

        У нас в `_run()` стоит `ex.shutdown(wait=True, ...)`: `_worker`
        не вернётся, пока все воркеры не завершатся. Если shutdown
        сокетов выбил recv() — воркеры умрут за миллисекунды, `_worker`
        завершится, watchdog увидит `not worker.is_alive()` и НЕ вызовет
        `os._exit`. Всё завершается нормально.

        `os._exit` — escape только для патологического случая, когда
        shutdown сокета по какой-то причине не выбил recv() из ядра
        (редкость, но бывает на Windows с HTTPS через прокси).
        """
        self.engine.request_stop()
        self.destroy()

        worker = self.engine._worker

        def watchdog():
            if worker is not None and worker.is_alive():
                worker.join(timeout=CLOSE_GRACE_SECONDS)
            if worker is None or not worker.is_alive():
                # _worker завершился → воркеры executor'а join'нуты
                # через ex.shutdown(wait=True) → _python_exit ничего
                # не заблокирует. Выходим штатно.
                return
            # Патологический случай: recv() не выбило. Жёсткий выход.
            os._exit(0)

        threading.Thread(target=watchdog, daemon=True).start()

    # ---------- Дренаж очереди ----------
    def _poll_results(self):
        processed = 0
        while processed < GUI_ROWS_PER_TICK:
            try:
                item = self.result_queue.get_nowait()
            except queue.Empty:
                break
            if item == "__DONE__":
                self._done_seen = True
                continue
            if item is None:
                continue
            self._add_result_row(item)
            processed += 1

        if processed:
            self.progress["value"] = self.done_count
            self.progress_label.config(
                text=f"{self.done_count} / {self.total_urls}")

        # __DONE__ кладётся последним, продюсеров больше нет —
        # empty() после _done_seen достоверен.
        if self._done_seen and self.result_queue.empty():
            self._finalize()
            return

        self.after(GUI_POLL_INTERVAL_MS, self._poll_results)

    def _add_result_row(self, res):
        idx = res["index"]
        iid = self._iid_by_index.get(idx)
        if iid is None:
            return
        status = res["status"] if res["status"] is not None else "—"
        time_s = f"{res['time']:.3f}" if res["time"] else "—"
        hops = len(res.get("chain", []))

        self.tree.item(iid, values=(
            idx + 1, res["input"], status, time_s, hops,
            res["final"] or "", format_chain(res["chain"]),
            res["error"] or "",
        ))

        tag = None
        if res["error"]:
            tag = "err"
        elif isinstance(res["status"], int):
            if 200 <= res["status"] < 300:
                tag = "ok"
            elif 300 <= res["status"] < 400:
                tag = "redir"
            elif 400 <= res["status"] < 500:
                tag = "warn"
            elif 500 <= res["status"] < 600:
                tag = "err"
        self.tree.item(iid, tags=(tag,) if tag else ())

        self.results[idx] = res
        self.done_count += 1

    def _finalize(self):
        self.start_btn.config(state="normal")
        self.stop_btn.config(state="disabled")
        self.clear_btn.config(state="normal")
        remaining = self.total_urls - self.done_count
        if remaining > 0:
            self.status_var.set(
                f"Остановлено. Обработано: {self.done_count} из "
                f"{self.total_urls} (пропущено: {remaining})")
        else:
            self.status_var.set(f"Готово. Обработано: {self.done_count}")


if __name__ == "__main__":
    App().mainloop()