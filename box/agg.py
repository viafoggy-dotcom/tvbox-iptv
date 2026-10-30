#!/usr/bin/env python3
"""Сборщик плейлиста российских каналов из m3u.su с постоянной проверкой потоков.

  python3 agg.py config.json           — сервис: проверки по расписанию + HTTP-сервер
  python3 agg.py config.json --once    — одна полная проверка, файлы в out_dir

Уровни проверки (чтобы не гонять гигабайты на приставке):
  main_every_min  — текущие потоки основных каналов; упавший сразу меняется на лучший запасной
  all_every_min   — то же для всех каналов
  full_every_min  — заново скачать источники, проверить все варианты, переизбрать лучших

Отдаёт: /playlist.m3u (основные, затем все остальные), /main.m3u, /channels.json и /epg.json (свой плеер),
/status, /status.json.
Кинозалы smotret-online.tv (ключ kinozals в config) — группой в конце playlist.m3u, потоки через /kz/.
"""
import calendar
import concurrent.futures as cf
import gzip
import hashlib
import html
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, urljoin, urlsplit

from channels import MAIN

DEFAULT_UA = "Mozilla/5.0 (Linux; Android 10; TV) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
HISTORY = 12          # сколько последних проверок помнить на поток
SEG_SMALL = 65536     # байт сегмента для проверки «живой ли»
SEG_SPEED = 524288    # байт сегмента для замера скорости (основные каналы)
log_lock = threading.Lock()


def log(*a):
    with log_lock:
        print(time.strftime("%m-%d %H:%M:%S"), *a, flush=True)


# ---------- названия ----------

SHIFT = re.compile(r"(\+\s?\d+|\bмск\s?[+-]\s?\d+|\bmsk\s?[+-]\s?\d+)", re.I)
TAGS = re.compile(r"\b(hd|fhd|uhd|sd|4k|8k|hevc|h265|orig|original|50fps|(?:tv|тв)(?=\s*$)|live|online|лайт|резерв|backup|\d{3,4}p)\b", re.I)


def norm(name):
    s = name.lower().replace("ё", "е")
    s = re.sub(r"\(.*?\)|\[.*?\]", " ", s)
    s = SHIFT.sub(" ", s)
    s = TAGS.sub(" ", s)
    return re.sub(r"[^0-9a-zа-я]+", "", s)


def display(name):
    """«Kuban 24 SD (576p) [Geo-blocked]» → «Kuban 24»"""
    s = re.sub(r"\[[^\]]*\]|\(\d{3,4}[pi]\)|\((Russia|Россия)\)", " ", name)
    s = re.sub(r"\s+\b(HD|FHD|UHD|SD|orig)\b\s*$", " ", s.strip(), flags=re.I)
    return re.sub(r"\s+", " ", s).strip() or name


def is_shift(name):
    return bool(SHIFT.search(name))


def is_regional(name):
    return bool(re.search(r"\((?!.*\b(hd|fhd|sd|4k|orig)\b)[^)]*\)", name, re.I))


ALIAS = {}
for order, (grp, canon, al) in enumerate(MAIN):
    for a in al + [canon]:
        ALIAS.setdefault(norm(a), (order, grp, canon))
MAIN_GROUPS = list(dict.fromkeys(g for g, _, _ in MAIN))

# группы iptv-org (английские) и разнобой источников → единые русские
GROUPS = [
    (r"местн|регион|беларус", "Региональные"),
    (r"kids|animation|детск", "Детские"),
    (r"news|business|legislative|информац", "Новости"),
    (r"sport|спорт", "Спорт"),
    (r"music|музык", "Музыка"),
    (r"movie|series|кино|classic|comedy", "Кино и сериалы"),
    (r"documentary|education|travel|outdoor|culture|science|познават|образоват|культур", "Познавательные"),
    (r"religious|христиан", "Религия"),
    (r"lifestyle|cooking|auto|shop|хобби", "Хобби"),
    (r"entertainment|развлекат", "Развлекательные"),
]


def group_of(e):
    g = e["attrs"].get("group-title", "")
    for pat, name in GROUPS:
        if re.search(pat, g, re.I):
            return name
    return "Разное"


# ---------- источники ----------

def fetch(url, ua=DEFAULT_UA, n=None, timeout=10, referer=None):
    h = {"User-Agent": ua}
    if referer:
        h["Referer"] = referer
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read(n) if n else r.read()
        ctype = r.headers.get("Content-Type", "")
        if body[:2] == b"\x1f\x8b" and ("mpegurl" in ctype.lower() or r.geturl().split("?")[0].endswith((".m3u8", ".m3u"))):
            body = gzip.decompress(body)   # некоторые CDN (cdn.ntv.ru) жмут плейлист без Content-Encoding
        return body, r.geturl(), ctype


def parse_m3u(text, src):
    out, cur = [], None
    for raw in text.splitlines():
        line = raw.strip().lstrip("﻿")
        if line.startswith("#EXTINF"):
            head, _, name = line.rpartition(",")
            cur = {"name": name.strip(), "attrs": dict(re.findall(r'([\w-]+)="([^"]*)"', head)), "ua": None, "ref": None, "src": src}
        elif cur and line.startswith("#EXTVLCOPT:"):
            k, _, v = line[11:].partition("=")
            v = v.strip().strip('"')
            if k == "http-user-agent":
                cur["ua"] = v
            elif k == "http-referrer":
                cur["ref"] = v
        elif cur and line and not line.startswith("#"):
            cur["url"] = line
            out.append(cur)
            cur = None
    return out


def load_sources(cfg):
    """Источники с копией последней удачной версии: при сбое m3u.su идём по запомненному
    настоящему адресу, а если и он не отвечает (или источник удалён) — берём копию."""
    cdir = cfg.get("source_cache", "sources")
    keep = cfg.get("source_cache_days", 60) * 86400
    os.makedirs(cdir, exist_ok=True)
    meta_path = os.path.join(cdir, "meta.json")
    try:
        meta = json.load(open(meta_path))
    except Exception:
        meta = {}
    entries, m3u_down = [], False
    for s in cfg["sources"]:
        url = s if s.startswith("http") else f"https://m3u.su/{s}"
        path = os.path.join(cdir, re.sub(r"[^\w.-]", "_", s)[-80:] + ".m3u")
        real = meta.get(s, {}).get("real")
        text, err = None, None
        tries = [url] + ([real] if real and real != url else [])
        if m3u_down and real:
            tries = [real]   # m3u.su уже не ответил в этом проходе — не ждём таймаут снова
        for u in tries:
            try:
                body, got, _ = fetch(u, timeout=30)
                t = body.decode("utf-8", "replace")
                if "#EXTINF" not in t:
                    raise ValueError("ответ без каналов")
                text = t
                with open(path + ".tmp", "w", encoding="utf-8") as f:
                    f.write(t)
                os.replace(path + ".tmp", path)
                meta[s] = {"real": got, "t": int(time.time())}
                break
            except Exception as ex:
                err = ex
                if "m3u.su/" in u and not isinstance(ex, urllib.error.HTTPError):
                    m3u_down = True
        if text is None:
            age = time.time() - meta.get(s, {}).get("t", 0)
            if os.path.exists(path) and age < keep:
                text = open(path, encoding="utf-8").read()
                log(f"источник {s}: недоступен ({err}), копия от {time.strftime('%d.%m %H:%M', time.localtime(meta[s]['t']))}")
            else:
                log(f"источник {s}: ошибка {err}, копии нет")
                continue
        e = parse_m3u(text, s)
        entries += e
        log(f"источник {s}: {len(e)} записей")
    with open(meta_path + ".tmp", "w") as f:
        json.dump(meta, f)
    os.replace(meta_path + ".tmp", meta_path)
    return entries


def group_channels(entries, main_only=False):
    chans = {}
    for e in entries:
        if not e["url"].startswith("http") or is_shift(e["name"]):
            continue
        k = norm(e["name"])
        if not k:
            continue
        main = ALIAS.get(k)
        if main_only and not main:
            continue
        key = main[2] if main else k
        c = chans.setdefault(key, {"key": key, "main": main, "name": main[2] if main else display(e["name"]),
                                   "cand": [], "tvg": {}, "group": main[1] if main else None})
        if not main and re.search("[а-я]", e["name"], re.I) and not re.search("[а-я]", c["name"], re.I):
            c["name"] = display(e["name"])   # русское название лучше латиницы iptv-org
        if e["url"] not in {x["url"] for x in c["cand"]}:
            c["cand"].append(e)
        a = e["attrs"]
        if a.get("tvg-id") and "@" not in a["tvg-id"] and not c["tvg"].get("id"):   # предпочитаем id iptvx.one
            c["tvg"]["id"] = a["tvg-id"]
        if a.get("tvg-logo") and not c["tvg"].get("logo"):
            c["tvg"]["logo"] = a["tvg-logo"]
        if not c["group"]:
            c["group"] = group_of(e)
    return chans


# ---------- проверка потока ----------

def probe(e, seg_bytes=SEG_SMALL):
    """master → лучший вариант ≤1080p → сегмент. Возвращает ok, height, need/got (кбит/с), ms, err."""
    ua, ref = e["ua"] or DEFAULT_UA, e["ref"]
    t0 = time.time()
    res = {"ok": False, "height": 0, "need": 0, "got": 0, "ms": 0, "err": "", "t": int(t0)}
    try:
        url = e["url"]
        for _ in range(4):
            body, url, ctype = fetch(url, ua, 262144, 8, ref)
            text = body[:200000].decode("utf-8", "replace")
            if "#EXTM3U" not in text:
                if body[:1] == b"G" or "video" in ctype or "octet" in ctype or url.endswith(".mpd") or "<MPD" in text:
                    res.update(ok=len(body) > 2000, err="" if len(body) > 2000 else "пусто")
                else:
                    res["err"] = f"не поток ({ctype or 'нет типа'})"
                break
            res["hls"] = True
            lines = text.splitlines()
            if any(l.startswith("#EXT-X-STREAM-INF") for l in lines):
                vars_ = []
                for i, l in enumerate(lines):
                    if l.startswith("#EXT-X-STREAM-INF"):
                        h = re.search(r"RESOLUTION=\d+x(\d+)", l)
                        bw = re.search(r"[^-]BANDWIDTH=(\d+)", l)
                        nxt = next((x for x in lines[i + 1:] if x and not x.startswith("#")), None)
                        if nxt:
                            vars_.append((int(h[1]) if h else 0, int(bw[1]) if bw else 0, nxt))
                ok_v = [v for v in vars_ if v[0] <= 1080] or vars_
                h, bw, nxt = max(ok_v, key=lambda v: (v[0], v[1]))
                res["height"], res["need"] = h, bw // 1000
                url = urljoin(url, nxt)
                continue
            segs = [l for l in lines if l and not l.startswith("#")]
            if not segs:
                res["err"] = "нет сегментов"
                break
            seg = urljoin(url, segs[-2] if len(segs) > 2 else segs[-1])
            ts = time.time()
            data, _, _ = fetch(seg, ua, seg_bytes, 10, ref)
            dt = max(time.time() - ts, 0.05)
            if len(data) < 4096:
                res["err"] = "сегмент пустой"
                break
            if seg_bytes >= SEG_SPEED:
                res["got"] = int(len(data) * 8 / dt / 1000)
            res["ok"] = True
            break
    except Exception as ex:
        res["err"] = type(ex).__name__ + (f" {ex.code}" if hasattr(ex, "code") else "")
    res["ms"] = int((time.time() - t0) * 1000)
    return res


# ---------- кинозалы smotret-online.tv ----------
# Ссылки на потоки подписаны и живут около часа, поэтому в плейлисте — адрес этого сервера:
# /kz/<slug>.m3u8 добывает свежую ссылку (страница → iframe плеера → m3u8) и проксирует поток
# с заголовками браузера: без них плеер cdntvmedia отвечает 403 (как и на IP датацентров).

KZ_GROUP = "Кинозалы"
KZ_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
         "(KHTML, like Gecko) Version/18.6 Safari/605.1.15")
KZ_IFRAME = re.compile(r'<iframe[^>]+src=["\']([^"\']+)["\']', re.I)
KZ_M3U8 = re.compile(r'https?:(?:\\?/){2}[^"\'\s<>]+?\.m3u8[^"\'\s<>]*', re.I)


def kz_origin(url):
    p = urlsplit(url)
    return f"{p.scheme}://{p.netloc}"


def kz_page(url, referer=None):
    """Страница так, как её грузит Safari: документом или iframe (Referer чужому сайту — только origin)."""
    h = {"User-Agent": KZ_UA, "Accept": "text/html,application/xhtml+xml,*/*;q=0.8", "Accept-Language": "ru",
         "Sec-Fetch-Mode": "navigate", "Sec-Fetch-Dest": "iframe" if referer else "document", "Sec-Fetch-Site": "none"}
    if referer:
        same = kz_origin(referer) == kz_origin(url)
        h["Referer"] = referer if same else kz_origin(referer) + "/"
        h["Sec-Fetch-Site"] = "same-origin" if same else "cross-site"
    with urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=15) as r:
        return r.read().decode(r.headers.get_content_charset() or "utf-8", "replace")


def kz_open(url, ref, timeout=15):
    """Поток или сегмент: CDN ждёт Origin/Referer плеера, со страницы которого взята ссылка."""
    o = kz_origin(ref)
    h = {"User-Agent": KZ_UA, "Accept": "*/*", "Accept-Language": "ru", "Origin": o, "Referer": o + "/"}
    return urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout)


def kz_channels(index):
    page = kz_page(index)
    out, seen = [], set()
    for m in re.finditer(r'<div class="tv-schedule"([^>]+)>', page):
        a = dict(re.findall(r'data-([\w-]+)="([^"]*)"', m.group(1)))
        slug = a.get("link", "").rsplit("/", 1)[-1].removesuffix(".html")
        if re.fullmatch(r"[\w-]+", slug) and slug not in seen:
            seen.add(slug)
            out.append({"slug": slug, "page": urljoin(index, a["link"]), "logo": a.get("logo", ""),
                        "name": html.unescape(a.get("custom-name") or a.get("channel-id") or slug), "ok": None})
    return out


def kz_resolve(page):
    """Страница канала → вложенные iframe → первая ссылка на m3u8. Возвращает (m3u8, адрес плеера)."""
    url, ref = page, None
    for _ in range(5):
        text = kz_page(url, ref)
        m = KZ_M3U8.search(html.unescape(text))   # ok.ru: JSON с hlsMasterPlaylistUrl в атрибуте через &quot;
        if m:
            link = m.group(0).replace("\\/", "/").rstrip("\\")
            # плееры cdntvtech: playersjs.php?file=<закодированная ссылка на livetv.php?key=….m3u8>
            inner = parse_qs(urlsplit(link).query).get("file", [""])[0]
            return (inner if ".m3u8" in inner else link), url
        frames = [urljoin(url, html.unescape(s)) for s in KZ_IFRAME.findall(text)]
        frames = [f for f in frames if not re.search(r"google|yandex|adfinity", f)]
        if not frames:
            raise LookupError(f"нет ссылки на поток: {url}")
        url, ref = frames[0], url
    raise LookupError(f"слишком глубокие iframe: {page}")


def kz_expiry(link):
    """/<подпись>/<канал>/<unix-срок>/index.m3u8 или ?wmsAuthSign=<подпись>-<unix-срок>-…"""
    p = urlsplit(link)
    for part in p.path.split("/") + parse_qs(p.query).get("wmsAuthSign", [""])[0].split("-"):
        if re.fullmatch(r"1\d{9}", part):
            return int(part)
    return None


class KzLinks:
    """Свежие ссылки по каналам; живут до срока из подписи минус 2 мин."""

    def __init__(self):
        self.lock = threading.Lock()
        self.pages, self.links = {}, {}
        self.index = None   # адрес kinozals.html: страница канала рядом с ним, пока список не загружен

    def get(self, slug):
        now = time.time()
        with self.lock:
            hit = self.links.get(slug)
            page = self.pages.get(slug) or (self.index and urljoin(self.index, slug + ".html"))
        if hit and hit[2] > now:
            return hit[0], hit[1]
        if not page:
            raise LookupError(f"неизвестный кинозал {slug}")
        link, ref = kz_resolve(page)
        exp = kz_expiry(link)
        ttl = min(exp - now - 120, 3600) if exp else 600
        with self.lock:
            self.links[slug] = (link, ref, now + max(ttl, 30))
        return link, ref

    def drop(self, slug):
        with self.lock:
            self.links.pop(slug, None)


KZ = KzLinks()


def kz_rewrite(text, base_url, ref, local, slug):
    """Все адреса плейлиста — через нас. Варианты master-плейлиста ссылаются на /kz/<slug>.m3u8?v=N,
    чтобы при обновлении плейлиста подпись бралась свежая, а не та, что была при старте."""
    def wrap(u):
        return f"{local}/kz/r?u={quote(urljoin(base_url, u), safe='')}&ref={quote(ref, safe='')}"
    master = bool(slug) and "#EXT-X-STREAM-INF" in text
    out, var = [], 0
    for line in text.splitlines():
        if line.startswith("#"):
            line = re.sub(r'URI="([^"]+)"', lambda m: f'URI="{wrap(m.group(1))}"', line)
        elif line.strip():
            line = f"{local}/kz/{slug}.m3u8?v={var}" if master else wrap(line.strip())
            var += master
        out.append(line)
    return "\n".join(out) + "\n"


def kz_handle(h):
    """GET /kz/<slug>.m3u8[?v=N] — плейлист канала; /kz/r?u=…&ref=… — сегменты и ключи."""
    path, _, query = h.path.partition("?")
    q = parse_qs(query)
    local = f"http://{h.headers.get('Host')}"

    def send(body, ctype, code=200):
        h.send_response(code)
        h.send_header("Content-Type", ctype)
        h.send_header("Content-Length", str(len(body)))
        h.end_headers()
        h.wfile.write(body)

    def playlist(slug, v):
        link, ref = KZ.get(slug)
        with kz_open(link, ref) as r:
            url, text = r.geturl(), r.read().decode("utf-8", "replace")
        if v is not None and "#EXT-X-STREAM-INF" in text:
            variants = [l.strip() for l in text.splitlines() if l.strip() and not l.startswith("#")]
            url = urljoin(url, variants[min(int(v), len(variants) - 1)])
            with kz_open(url, ref) as r:
                url, text = r.geturl(), r.read().decode("utf-8", "replace")
        send(kz_rewrite(text, url, ref, local, slug).encode(), "application/vnd.apple.mpegurl")

    try:
        m = re.fullmatch(r"/kz/([\w-]+)\.json", path)   # отладка: какую ссылку добыли и откуда
        if m:
            KZ.drop(m[1])
            link, ref = KZ.get(m[1])
            try:
                with kz_open(link, ref) as r:
                    st = f"{r.status} {r.headers.get('Content-Type', '')}"
            except urllib.error.HTTPError as ex:
                st = f"{ex.code} {ex.read(300).decode('utf-8', 'replace')}"
            return send(json.dumps({"m3u8": link, "referer": ref, "stream": st}, ensure_ascii=False).encode(), "application/json")
        m = re.fullmatch(r"/kz/([\w-]+)\.m3u8", path)
        if m:
            try:
                return playlist(m[1], q.get("v", [None])[0])
            except urllib.error.HTTPError:
                KZ.drop(m[1])   # подпись истекла раньше срока или сменился IP — добываем заново
                return playlist(m[1], q.get("v", [None])[0])
        if path == "/kz/r" and "u" in q:
            ref = q.get("ref", [""])[0]
            with kz_open(q["u"][0], ref, timeout=20) as r:
                ctype = r.headers.get("Content-Type", "")
                head = r.read(7)   # livetv.php отдаёт плейлисты без mpegurl и без .m3u8 в пути
                if head == b"#EXTM3U" or "mpegurl" in ctype.lower():
                    text = (head + r.read()).decode("utf-8", "replace")
                    return send(kz_rewrite(text, r.geturl(), ref, local, "").encode(), "application/vnd.apple.mpegurl")
                h.send_response(200)
                # tvcdnpotok отдаёт сегменты как text/plain — часть плееров тогда не показывает картинку
                h.send_header("Content-Type", "video/mp2t" if not ctype or ctype.startswith("text/") else ctype)
                if r.headers.get("Content-Length"):
                    h.send_header("Content-Length", r.headers["Content-Length"])
                h.end_headers()
                h.wfile.write(head)
                shutil.copyfileobj(r, h.wfile, 65536)
                return
        send(b"not found\n", "text/plain", 404)
    except (BrokenPipeError, ConnectionResetError):
        pass
    except Exception as ex:
        log(f"кинозал {h.path[:80]}: {ex!r}")
        try:
            send(f"{ex}\n".encode(), "text/plain", 502)
        except OSError:
            pass


# ---------- состояние и выбор ----------

class Agg:
    def __init__(self, cfg):
        self.cfg = cfg
        self.path = cfg.get("state", "state.json")
        try:
            d = json.load(open(self.path))
        except Exception:
            d = {}
        self.hist = d.get("hist", {})        # url → [1,0,1…]
        self.last = d.get("last", {})        # url → результат последней проверки
        self.chosen = d.get("chosen", {})    # канал → url
        self.chans = {}
        self.kz = []                         # кинозалы: [{"slug", "name", "logo", "page", "ok"}]
        self.lock = threading.Lock()
        self.busy_since = 0

    def save(self):
        tmp = self.path + ".tmp"
        json.dump({"hist": self.hist, "last": self.last, "chosen": self.chosen}, open(tmp, "w"), ensure_ascii=False)
        os.replace(tmp, self.path)

    def rate(self, url):
        h = self.hist.get(url, [])
        return sum(h) / len(h) if h else 0

    def run_probes(self, items):
        """items: [(entry, seg_bytes)] — проверить параллельно и записать в историю."""
        with cf.ThreadPoolExecutor(self.cfg.get("workers", 12)) as ex:
            res = list(ex.map(lambda it: probe(*it), items))
        for (e, _), r in zip(items, res):
            u = e["url"]
            prev = self.last.get(u, {})
            if r["ok"] and not r["got"] and prev.get("got"):   # маленькая проба скорость не меряет — держим прошлую
                r["got"] = prev["got"]
            self.last[u] = r
            h = self.hist.setdefault(u, [])
            h.append(1 if r["ok"] else 0)
            del h[:-HISTORY]

    def score(self, e, cur):
        r = self.last.get(e["url"])
        if not r or not r["ok"]:
            return (-1,)
        slow = r["need"] and r["got"] and r["got"] < r["need"] * 1.3
        return (
            1,
            round(self.rate(e["url"]), 1),            # стабильность за последние проверки
            0 if slow else 1,                         # успевает ли сервер отдавать поток
            0 if is_regional(e["name"]) else 1,
            min(r["height"] or (1080 if re.search(r"\b(hd|fhd)\b", e["name"], re.I) else 576), 1080),
            1 if e["url"] == cur else 0,              # не скачем между равными
            -r["ms"],
        )

    def pick(self, c):
        cur = self.chosen.get(c["key"])
        best = max(c["cand"], key=lambda e: self.score(e, cur))
        if not self.last.get(best["url"], {}).get("ok"):
            return None
        if cur and cur != best["url"]:
            log(f"  {c['name']}: → {best['src']} {best['url']}")
        self.chosen[c["key"]] = best["url"]
        return best

    # --- уровни ---

    def full(self):
        chans = group_channels(load_sources(self.cfg), self.cfg.get("main_only", False))
        if not chans:
            log("источники пусты — оставляю прошлый список")
            return
        self.chans = chans
        flat = [e for c in chans.values() for e in c["cand"]]
        log(f"полная проверка: каналов {len(chans)}, потоков {len(flat)}")
        self.run_probes([(e, SEG_SMALL) for e in flat])
        # параллельный замер делит канал — у основных перемеряем лучших кандидатов по одному
        for c in chans.values():
            if c["main"]:
                cur = self.chosen.get(c["key"])
                top = sorted(c["cand"], key=lambda e: self.score(e, cur), reverse=True)[:self.cfg.get("retest_top", 3)]
                for e in top:
                    if self.last[e["url"]]["ok"]:
                        self.run_probes([(e, SEG_SPEED)])
        live = {e["url"] for e in flat}
        self.hist = {u: h for u, h in self.hist.items() if u in live}
        self.last = {u: r for u, r in self.last.items() if u in live}
        for c in chans.values():
            self.pick(c)
        self.kz_refresh()

    def kz_refresh(self):
        """Список кинозалов со страницы и проверка, что у каждого добывается живой плейлист."""
        index = self.cfg.get("kinozals")
        if not index:
            self.kz = []
            return
        try:
            kz = kz_channels(index)
        except Exception as ex:
            log(f"кинозалы: список недоступен ({ex!r}), оставляю прошлый")
            return
        with KZ.lock:
            KZ.pages = {c["slug"]: c["page"] for c in kz}

        def check(c):
            try:
                link, ref = KZ.get(c["slug"])
                with kz_open(link, ref) as r:
                    return b"#EXTM3U" in r.read(4096)
            except Exception:
                KZ.drop(c["slug"])
                return False
        with cf.ThreadPoolExecutor(min(self.cfg.get("workers", 12), 6)) as ex:
            for c, ok in zip(kz, ex.map(check, kz)):
                c["ok"] = ok
        self.kz = kz
        log(f"кинозалы: живых {sum(c['ok'] for c in kz)} из {len(kz)}")

    def recheck(self, main_only):
        """Перечитать источники (ссылки с токенами меняются), проверить новые ссылки и текущие потоки;
        у упавших — все варианты канала."""
        fresh = group_channels(load_sources(self.cfg), self.cfg.get("main_only", False))
        if fresh:
            self.chans = fresh
        chans = [c for c in self.chans.values() if c["main"] or not main_only]
        self.run_probes([(e, SEG_SMALL) for c in chans for e in c["cand"] if e["url"] not in self.last])
        cur = [(e, SEG_SPEED if c["main"] else SEG_SMALL) for c in chans for e in c["cand"] if e["url"] == self.chosen.get(c["key"])]
        self.run_probes(cur)
        dead = [c for c in chans if not any(e["url"] == self.chosen.get(c["key"]) and self.last[e["url"]]["ok"] for e in c["cand"])]
        self.run_probes([(e, SEG_SMALL) for c in dead for e in c["cand"] if e["url"] != self.chosen.get(c["key"])])
        for c in dead:
            self.pick(c)
        log(f"проверка {'основных' if main_only else 'всех'}: {len(chans)} каналов, упало {len(dead)}")

    # --- выход ---

    def write(self, took):
        cfg, out = self.cfg, self.cfg["out_dir"]
        os.makedirs(out, exist_ok=True)
        hdr = f'#EXTM3U url-tvg="{cfg["epg"]}" x-tvg-url="{cfg["epg"]}"'
        gorder = {g: i for i, g in enumerate(MAIN_GROUPS)}

        def key(c):   # группы основных — первыми; внутри группы сначала основные по каталогу, затем по алфавиту
            return (gorder.get(c["group"], 99), c["group"], 0 if c["main"] else 1, c["main"][0] if c["main"] else 0, c["name"].lower())

        report, main_lines, all_lines, player = [], [], [], []
        for c in sorted(self.chans.values(), key=key):
            url = self.chosen.get(c["key"])
            best = next((e for e in c["cand"] if e["url"] == url), None)
            r = self.last.get(url, {}) if best else {}
            ok = bool(r.get("ok"))
            report.append({
                "channel": c["name"], "group": c["group"], "main": bool(c["main"]), "ok": ok,
                "alive": sum(1 for e in c["cand"] if self.last.get(e["url"], {}).get("ok")), "total": len(c["cand"]),
                "url": url if ok else None, "src": best["src"] if ok else None,
                "height": r.get("height", 0), "need": r.get("need", 0), "got": r.get("got", 0), "ms": r.get("ms", 0),
                "rate": self.rate(url) if ok else 0,
                "errors": sorted({self.last.get(e["url"], {}).get("err", "") for e in c["cand"]} - {""}),
            })
            if not ok:
                continue
            attrs = (f'tvg-id="{c["tvg"].get("id", "")}" tvg-name="{c["name"]}" tvg-logo="{c["tvg"].get("logo", "")}" '
                     f'group-title="{c["group"]}"')
            lines = [f"#EXTINF:-1 {attrs},{c['name']}"]
            if best["ua"]:
                lines.append(f"#EXTVLCOPT:http-user-agent={best['ua']}")
            if best["ref"]:
                lines.append(f"#EXTVLCOPT:http-referrer={best['ref']}")
            lines.append(url)
            all_lines += lines
            # для своего плеера: выбранный поток первым, затем живые запасные по убыванию качества
            alive = sorted((e for e in c["cand"] if self.last.get(e["url"], {}).get("ok")),
                           key=lambda e: (e["url"] == url, self.score(e, url)), reverse=True)[:cfg.get("player_streams", 10)]
            player.append({"n": len(player) + 1, "name": c["name"], "group": c["group"], "main": bool(c["main"]),
                           "tvg": c["tvg"].get("id", ""), "logo": c["tvg"].get("logo", ""),
                           "streams": [{"url": e["url"], "ua": e["ua"] or "", "ref": e["ref"] or "",
                                        "hls": bool(self.last[e["url"]].get("hls"))} for e in alive]})
            if c["main"]:
                main_lines += lines
        # кинозалы: если не прошёл ни один (сайт лёг на время проверки) — оставляем все, адреса-то наши
        local = cfg.get("kz_base") or f"http://127.0.0.1:{cfg.get('port', 8090)}"
        any_ok = any(c["ok"] for c in self.kz)
        for c in self.kz:
            url = f"{local}/kz/{c['slug']}.m3u8"
            report.append({"channel": c["name"], "group": KZ_GROUP, "main": False, "ok": bool(c["ok"]),
                           "alive": int(bool(c["ok"])), "total": 1, "url": url, "src": "smotret-online",
                           "height": 0, "need": 0, "got": 0, "ms": 0, "rate": 1 if c["ok"] else 0,
                           "errors": [] if c["ok"] else ["ссылка не добыта"]})
            if any_ok and not c["ok"]:
                continue
            all_lines += [f'#EXTINF:-1 tvg-id="{c["slug"]}" tvg-name="{c["name"]}" tvg-logo="{c["logo"]}" '
                          f'group-title="{KZ_GROUP}",{c["name"]}', url]
            player.append({"n": len(player) + 1, "name": c["name"], "group": KZ_GROUP, "main": False,
                           "tvg": c["slug"], "logo": c["logo"], "streams": [{"url": url, "ua": "", "ref": "", "hls": True}]})
        for fn, ls in (("main.m3u", main_lines), ("playlist.m3u", all_lines)):
            tmp = os.path.join(out, fn + ".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                f.write("\n".join([hdr] + ls) + "\n")
            os.replace(tmp, os.path.join(out, fn))
        tmp = os.path.join(out, "channels.json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"updated": int(time.time()), "epg": cfg["epg"], "channels": player}, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, os.path.join(out, "channels.json"))
        status = {"updated": time.strftime("%Y-%m-%d %H:%M:%S"), "took_s": took, "channels": report}
        json.dump(status, open(os.path.join(out, "status.json"), "w"), ensure_ascii=False, indent=1)
        with open(os.path.join(out, "status.html"), "w", encoding="utf-8") as f:
            f.write(status_html(status))
        self.save()
        if cfg.get("log_copy") and os.path.exists("agg.log"):   # журнал виден через adb без перезапуска
            try:
                with open("agg.log", "rb") as f:
                    f.seek(max(0, os.path.getsize("agg.log") - 200_000))
                    data = f.read()
                with open(cfg["log_copy"] + ".tmp", "wb") as f:
                    f.write(data)
                os.replace(cfg["log_copy"] + ".tmp", cfg["log_copy"])
            except OSError:
                pass
        m = [x for x in report if x["main"]]
        log(f"записано: основные {sum(x['ok'] for x in m)}/{len(m)}, всего {sum(x['ok'] for x in report)}/{len(report)}")

    def step(self, level):
        t0 = self.busy_since = time.time()
        try:
            with self.lock:
                if level == "full" or not self.chans:
                    self.full()
                else:
                    self.recheck(main_only=(level == "main"))
                self.write(int(time.time() - t0))
        finally:
            self.busy_since = 0


def status_html(s):
    rows = []
    for x in s["channels"]:
        if not x["main"] and not x["ok"]:
            continue
        q = f'{x["height"]}p' if x["height"] else ""
        sp = f'{x["got"] // 1000} / {x["need"] // 1000}' if x["need"] else (f'{x["got"] // 1000}' if x["got"] else "")
        rows.append(f'<tr class="{"ok" if x["ok"] else "bad"}"><td>{html.escape(x["group"])}</td><td>{html.escape(x["channel"])}</td>'
                    f'<td>{x["alive"]}/{x["total"]}</td><td>{q}</td><td>{sp}</td><td>{int(x["rate"] * 100)}%</td>'
                    f'<td>{html.escape(x["src"] or ", ".join(x["errors"])[:60])}</td></tr>')
    n_ok = sum(x["ok"] for x in s["channels"])
    return f"""<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>IPTV статус</title><style>body{{font:14px system-ui;margin:16px}}td,th{{padding:3px 8px;text-align:left}}
tr.bad{{color:#b00}}tr:nth-child(even){{background:#f3f3f3}}</style>
<h3>Обновлено {s['updated']} · живых {n_ok} из {len(s['channels'])}</h3>
<p><a href="/playlist.m3u">playlist.m3u</a> · <a href="/main.m3u">main.m3u</a> · <a href="/status.json">status.json</a></p>
<table><tr><th>Группа</th><th>Канал</th><th>Живых</th><th>Качество</th><th>Мбит есть / нужно</th><th>Стабильность</th><th>Источник</th></tr>
{''.join(rows)}</table>"""


# ---------- программа передач ----------
# Раз в epg_every_min качаем XMLTV (cfg["epg"], ~17 МБ gz, >1 млн передач) и оставляем только каналы
# из channels.json на окно [-3 ч, +48 ч] → epg.json {"updated", "ch": {имя канала: [[начало, конец, название], …]}}.
# Разбираем построчно байтами (у iptvx.one одна передача — одна строка): на приставке это в разы быстрее ElementTree.

EPG_CH = re.compile(rb'<channel id="([^"]+)">(.*)')
EPG_NAME = re.compile(rb"<display-name[^>]*>([^<]+)</display-name>")
EPG_PROG = re.compile(rb'<programme start="(\d{14}) ?([+-]\d{4})?" stop="(\d{14}) ?([+-]\d{4})?" channel="([^"]+)"')
EPG_TITLE = re.compile(rb"<title[^>]*>([^<]*)</title>")


def name_key(name):
    """Строже norm(): скобки («12 канал (Омск)») не выбрасываем, иначе регионы склеятся."""
    s = re.sub(r"\s+\b(hd|fhd|uhd|sd|orig)\b\s*$", "", name.lower().replace("ё", "е").strip())
    return re.sub(r"[^0-9a-zа-я]+", "", s)


def xmltv_ts(t, tz):
    ts = calendar.timegm((int(t[:4]), int(t[4:6]), int(t[6:8]), int(t[8:10]), int(t[10:12]), int(t[12:14]), 0, 0, 0))
    if tz:
        off = int(tz[1:3]) * 3600 + int(tz[3:5]) * 60
        ts -= off if tz[:1] == b"+" else -off
    return ts


def build_epg(cfg):
    out = cfg["out_dir"]
    chans = json.load(open(os.path.join(out, "channels.json"), encoding="utf-8"))["channels"]
    ids, by_name = {}, {}
    for c in chans:
        if c["tvg"]:
            ids.setdefault(c["tvg"], []).append(c["name"])
        by_name.setdefault(name_key(c["name"]), []).append(c["name"])
    own, by_dn = {}, {}                      # имя канала → программа по tvg-id / по названию
    want = None                              # id программы → имена каналов плейлиста
    progs, n_ch, t0 = {}, 0, time.time()
    lo, hi = time.time() - 3 * 3600, time.time() + 48 * 3600
    req = urllib.request.Request(cfg["epg"], headers={"User-Agent": DEFAULT_UA})
    with urllib.request.urlopen(req, timeout=60) as r, gzip.GzipFile(fileobj=r) as f:
        for line in f:
            if line.startswith(b"<programme"):
                if want is None:             # в XMLTV все <channel> идут до передач — сопоставляем один раз
                    want = {}
                    for name, cid in {**by_dn, **own}.items():
                        want.setdefault(cid, []).append(name)
                m = EPG_PROG.match(line)
                if not m or m.group(5) not in want:
                    continue
                a, b = xmltv_ts(m.group(1), m.group(2)), xmltv_ts(m.group(3), m.group(4))
                if b < lo or a > hi:
                    continue
                t = EPG_TITLE.search(line)
                title = html.unescape(t.group(1).decode("utf-8", "replace")).strip() if t else ""
                for name in want[m.group(5)]:
                    progs.setdefault(name, []).append([a, b, title])
            elif line.startswith(b"<channel"):
                m = EPG_CH.match(line)
                if not m:
                    continue
                n_ch += 1
                cid = m.group(1)
                for name in ids.get(cid.decode(), []):
                    own[name] = cid
                for dn in EPG_NAME.findall(m.group(2)):   # канал без своего tvg-id — по названию (первое совпадение)
                    for name in by_name.get(name_key(html.unescape(dn.decode("utf-8", "replace"))), []):
                        by_dn.setdefault(name, cid)
    for v in progs.values():
        v.sort()
    if not progs:
        raise ValueError(f"в программе ни одного нашего канала (каналов в файле {n_ch})")
    tmp = os.path.join(out, "epg.json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"updated": int(time.time()), "ch": progs}, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, os.path.join(out, "epg.json"))
    log(f"программа: {len(progs)} из {len(chans)} каналов, {sum(map(len, progs.values()))} передач, {int(time.time() - t0)} с")


def epg_loop(cfg):
    """Отдельная нить: разбор занимает минуту-другую процессора и не должен задерживать проверку потоков."""
    path = os.path.join(cfg["out_dir"], "epg.json")
    every = cfg.get("epg_every_min", 360) * 60
    while True:
        try:
            age = time.time() - os.path.getmtime(path) if os.path.exists(path) else every
            if age >= every and os.path.exists(os.path.join(cfg["out_dir"], "channels.json")):
                build_epg(cfg)
                continue
        except Exception as ex:
            log("программа:", repr(ex))
            time.sleep(1800)          # сайт программы лежит — старый epg.json покрывает ещё двое суток
        time.sleep(300)


# ---------- сервис ----------

def serve(cfg):
    out = os.path.abspath(cfg["out_dir"])
    KZ.index = cfg.get("kinozals")

    class H(SimpleHTTPRequestHandler):
        def __init__(self, *a, **k):
            super().__init__(*a, directory=out, **k)

        def do_GET(self):
            if self.path.startswith("/kz/"):
                return kz_handle(self)
            if self.path in ("/", "/status"):
                self.path = "/status.html"
            super().do_GET()

        def guess_type(self, path):
            return "audio/x-mpegurl; charset=utf-8" if str(path).endswith(".m3u") else super().guess_type(path)

        def log_message(self, *a):
            pass

    try:
        srv = ThreadingHTTPServer((cfg.get("listen", "0.0.0.0"), cfg.get("port", 8090)), H)
    except OSError as ex:
        log(f"порт {cfg.get('port', 8090)} занят ({ex}) — выход")
        os._exit(2)
    log(f"HTTP на :{srv.server_port}")
    srv.serve_forever()


# ---------- самообновление ----------
# Раз в update_every_min читаем <update_url>/manifest.json: {"version": N, "files": {"agg.py": "<sha256>", …}}.
# Новая версия скачивается в next/, сверяется по sha256 и проходит --selftest; затем выходим,
# а run.sh подменяет файлы и перезапускает (и откатывает, если новая версия сразу падает).

UPDATABLE = {"agg.py", "channels.py", "config.json", "run.sh"}


def local_version(name="version"):
    try:
        return int(open(name).read().strip())
    except Exception:
        return 0


def check_update(cfg):
    base = cfg.get("update_url", "").rstrip("/")
    if not base:
        return False
    man = json.loads(fetch(base + "/manifest.json", timeout=20)[0])
    ver = int(man["version"])
    if ver <= local_version() or ver == local_version("bad_version"):
        return False
    files = man["files"]
    if not set(files) <= UPDATABLE or "agg.py" not in files:
        raise ValueError(f"манифест с чужими файлами: {sorted(files)}")
    shutil.rmtree("next", ignore_errors=True)
    os.makedirs("next")
    for fn, sha in files.items():
        data = fetch(f"{base}/{fn}", timeout=30)[0]
        if hashlib.sha256(data).hexdigest() != sha:
            raise ValueError(f"{fn}: sha256 не совпал")
        open(os.path.join("next", fn), "wb").write(data)
        if fn.endswith(".sh"):
            os.chmod(os.path.join("next", fn), 0o755)
    for fn in ("agg.py", "channels.py", "config.json"):   # недостающее — из текущей версии
        if not os.path.exists(os.path.join("next", fn)):
            shutil.copy(fn, "next")
    r = subprocess.run([sys.executable, "agg.py", "config.json", "--selftest"], cwd="next",
                       capture_output=True, text=True, timeout=120)
    if r.returncode != 0:
        shutil.rmtree("next", ignore_errors=True)
        raise ValueError(f"версия {ver} не прошла самопроверку: {(r.stderr or r.stdout)[-300:]}")
    open("next/version", "w").write(str(ver))
    log(f"скачана версия {ver}, перезапуск")
    return True


def selftest(cfg):
    """Новая версия должна импортироваться, разбирать m3u и сводить синонимы."""
    e = parse_m3u('#EXTM3U\n#EXTINF:-1 group-title="Эфирные",Первый канал HD\nhttp://x/1.m3u8\n', "t")
    assert e and ALIAS[norm(e[0]["name"])][2] == "Первый канал"
    assert kz_expiry("https://x/2051/index.m3u8?wmsAuthSign=ab-1793284083-1i2") == 1793284083
    assert kz_expiry("https://x/sig/2051/1793298074/index.m3u8") == 1793298074
    pl = kz_rewrite("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nhi/index.m3u8\n", "https://c/2051/index.m3u8", "https://p/x", "http://l", "s")
    assert "http://l/kz/s.m3u8?v=0" in pl
    assert "/kz/r?u=" in kz_rewrite("#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nhi.m3u8\n", "https://c/", "https://p/x", "http://l", "")
    assert xmltv_ts(b"20261022120000", b"+0300") == 1792659600
    assert name_key("12 канал (Омск) HD") == "12каналомск"
    assert cfg["sources"] and cfg["out_dir"]
    Agg(dict(cfg, state="/nonexistent/state.json"))
    print("selftest ok")


def main():
    cfg_path = os.path.abspath(sys.argv[1])
    cfg = json.load(open(cfg_path))
    os.chdir(os.path.dirname(cfg_path))
    if "--selftest" in sys.argv:
        selftest(cfg)
        return
    agg = Agg(cfg)
    if "--once" in sys.argv:
        agg.step("full")
        return
    threading.Thread(target=serve, args=(cfg,), daemon=True).start()

    def watchdog():
        """Проверка висит дольше step_max_min (сеть «залипла», поток не отдаёт таймаут) — выходим,
        run.sh перезапустит. Плеер тем временем играет по своей копии списка."""
        limit = cfg.get("step_max_min", 30) * 60
        while True:
            time.sleep(60)
            if agg.busy_since and time.time() - agg.busy_since > limit:
                log(f"проверка висит дольше {limit // 60} мин — перезапуск")
                os._exit(3)

    threading.Thread(target=watchdog, daemon=True).start()
    if cfg.get("epg"):
        threading.Thread(target=epg_loop, args=(cfg,), daemon=True).start()
    every = {"full": cfg.get("full_every_min", 720), "all": cfg.get("all_every_min", 120), "main": cfg.get("main_every_min", 30)}
    due = {lvl: 0 for lvl in every}
    flag = cfg.get("restart_flag")
    next_update = time.time() + 300      # первая проверка обновлений — через 5 мин после старта
    while True:
        if flag and os.path.exists(flag):
            os.remove(flag)
            log("перезапуск по флагу")
            return
        if time.time() >= next_update:
            next_update = time.time() + cfg.get("update_every_min", 720) * 60
            try:
                with agg.lock:           # не прерываем идущую проверку
                    if check_update(cfg):
                        return
            except Exception as ex:
                log("обновление:", repr(ex))
                next_update = time.time() + 1800   # кэш raw.githubusercontent.com мог отдать старый файл — повторим скоро
        now = time.time()
        lvl = next((l for l in ("full", "all", "main") if now >= due[l]), None)
        if lvl:
            try:
                agg.step(lvl)
            except Exception as ex:
                log("ошибка:", repr(ex))
            # более полная проверка покрывает более лёгкие
            for l in ("full", "all", "main")[("full", "all", "main").index(lvl):]:
                due[l] = time.time() + every[l] * 60
        time.sleep(30)


if __name__ == "__main__":
    main()
