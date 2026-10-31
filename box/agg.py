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
Каналы smotret-online.tv (site_sections в config): разделы сайта = группы плейлиста, совпавшие каналы получают
поток сайта запасным, остальные добавляются; потоки сайта — через /kz/ со свежей подписью.
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
START_SLOW_MS = 3000  # плейлист + сегмент дольше — поток стартует медленно, ставим за быстрыми
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

TS_VIDEO = {0x1b: "h264", 0x24: "hevc", 0x02: "mpeg2", 0x01: "mpeg2"}


def ts_video_codec(data):
    """Кодек видео по PMT сегмента MPEG-TS. MPEG-2 телевизор Samsung в HLS не показывает (только звук)."""
    pmt = set()
    for i in range(0, len(data) - 187, 188):
        p = data[i:i + 188]
        if p[0] != 0x47 or not p[1] & 0x40:
            continue
        pid = ((p[1] & 0x1f) << 8) | p[2]
        off = 4 + (1 + p[4] if (p[3] >> 4) & 2 else 0)
        pl = p[off:]
        if not pl:
            continue
        pl = pl[1 + pl[0]:]
        if pid == 0 and pl[:1] == b"\x00":
            n = ((pl[1] & 0x0f) << 8) | pl[2]
            pmt.update(((pl[j + 2] & 0x1f) << 8) | pl[j + 3] for j in range(8, min(3 + n - 4, len(pl) - 3), 4))
        elif pid in pmt and pl[:1] == b"\x02":
            n = ((pl[1] & 0x0f) << 8) | pl[2]
            j = 12 + (((pl[10] & 0x0f) << 8) | pl[11])
            while j < min(3 + n - 4, len(pl) - 4):
                if pl[j] in TS_VIDEO:
                    return TS_VIDEO[pl[j]]
                j += 5 + (((pl[j + 3] & 0x0f) << 8) | pl[j + 4])
            return ""
    return ""


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
            res["vcodec"] = ts_video_codec(data)
            res["ok"] = True
            break
    except Exception as ex:
        res["err"] = type(ex).__name__ + (f" {ex.code}" if hasattr(ex, "code") else "")
    res["ms"] = int((time.time() - t0) * 1000)
    return res


# ---------- smotrim: master-плейлист из отдельных вариантов ----------
# Источники дают stream.smotrim.ru/hls2/<канал>/playlist_N.m3u8 — сразу один вариант (у «России 1» это 1080p,
# сегменты по 5 МБ), а общий playlist.m3u8 закрыт (403). Плеер тогда не может начать с лёгкого качества.
# /sm/<hls>/<канал>.m3u8 отдаёт master со всеми видеовариантами (playlist_1…9; _N с субтитрами .vtt пропускаем),
# BANDWIDTH — по размеру последнего сегмента. Сами варианты плеер берёт прямо со smotrim.

SM_URL = re.compile(r"^https?://stream\.smotrim\.ru/(hls2?)/([\w-]+)/playlist_\d+\.m3u8$")
SM_CACHE = {}   # (hls, канал) → (master, срок)


def sm_variant(base, i):
    """(BANDWIDTH, адрес) видеоварианта playlist_i или None."""
    try:
        text = fetch(f"{base}playlist_{i}.m3u8", timeout=6)[0].decode("utf-8", "replace")
    except Exception:
        return None
    segs = [l for l in text.splitlines() if l and not l.startswith("#")]
    durs = [float(x) for x in re.findall(r"#EXTINF:([\d.]+)", text)]
    if not segs or not segs[-1].split("?")[0].endswith(".ts"):
        return None   # субтитры (.vtt) и пустые
    bw = 0
    try:
        req = urllib.request.Request(urljoin(base, segs[-1]), headers={"User-Agent": DEFAULT_UA, "Range": "bytes=0-0"})
        with urllib.request.urlopen(req, timeout=6) as r:
            size = int(r.headers.get("Content-Range", "/0").rsplit("/", 1)[-1] or 0)
        bw = int(size * 8 / (durs[-1] if durs else 6))
    except Exception:
        pass
    return bw or i * 1_000_000, f"{base}playlist_{i}.m3u8"


def sm_master(hls, name, fresh_for=0):
    """fresh_for — пересобрать, если кэш истекает раньше чем через столько секунд (фоновый прогрев)."""
    hit = SM_CACHE.get((hls, name))
    if hit and hit[1] > time.time() + fresh_for:
        return hit[0]
    base = f"https://stream.smotrim.ru/{hls}/{name}/"
    with cf.ThreadPoolExecutor(9) as ex:
        variants = [v for v in ex.map(lambda i: sm_variant(base, i), range(1, 10)) if v]
    if not variants:
        raise LookupError(f"smotrim {hls}/{name}: нет вариантов")
    lines = ["#EXTM3U"]
    for bw, url in sorted(variants):
        lines += [f"#EXT-X-STREAM-INF:BANDWIDTH={bw}", url]
    master = "\n".join(lines) + "\n"
    SM_CACHE[(hls, name)] = (master, time.time() + 6 * 3600)   # набор вариантов меняется редко
    return master


def sm_prewarm(keys):
    """После каждой записи списка — собрать master заранее, чтобы переключение на канал не ждало ~8 с."""
    def run():
        for hls, name in keys:
            try:
                sm_master(hls, name, fresh_for=3600)
            except Exception as ex:
                log(f"smotrim {hls}/{name}: {ex!r}")
    threading.Thread(target=run, daemon=True).start()


# ---------- обёртка /w: первый плейлист — с NAS ----------
# Плеер Samsung (AVPlay) часто зависает на подготовке, если первый запрос идёт на внешний сервер (A/B на ТВ:
# напрямую ~половина запусков висит >20 с, плейлист с NAS — 4 из 4). /w?u=<адрес> отдаёт master: исходный
# (относительные ссылки → абсолютные) или собранный из одного варианта. Сегменты плеер по-прежнему берёт напрямую.

W_CACHE = {}   # адрес → (текст, срок)
# CDN, где ссылки из плейлиста привязаны к запросившему (mirtv: ?hls_proxy_host=…) — плейлист с NAS, а видео
# напрямую не сходятся. Такие потоки целиком идут через NAS (/kz/r, как потоки сайта): ~6 Мбит/с на канал.
RELAY_HOSTS = re.compile(r"(^|[.-])mirtv\.cdnvideo\.ru$")


def abs_playlist(text, base, link=lambda u: u):
    """Ссылки плейлиста (строки и URI="…") — абсолютные, каждая через link()."""
    out = []
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            line = link(urljoin(base, line))
        elif 'URI="' in line:
            line = re.sub(r'URI="([^"]+)"', lambda m: f'URI="{link(urljoin(base, m.group(1)))}"', line)
        out.append(line)
    return "\n".join(out) + "\n"


LIVE_KEEP = 8   # сегментов живого хвоста
HEAD_TAGS = ("#EXTM3U", "#EXT-X-VERSION", "#EXT-X-TARGETDURATION", "#EXT-X-MEDIA-SEQUENCE", "#EXT-X-PLAYLIST-TYPE",
             "#EXT-X-DISCONTINUITY-SEQUENCE", "#EXT-X-INDEPENDENT-SEGMENTS", "#EXT-X-ALLOW-CACHE", "#EXT-X-START")


def trim_live(text, keep=LIVE_KEEP):
    """Плейлист с перемоткой (Матч ТВ — 301 сегмент, webcaster — 454, Setanta — 3750) AVPlay разбирает
    секундами, на ТВ такие каналы стартовали за 10–12 с. Оставляем живой хвост; номер первого сегмента,
    счётчик разрывов, действующие ключ и инициализацию переносим, чтобы хвост читался как исходный."""
    if "#EXT-X-ENDLIST" in text or "#EXT-X-STREAM-INF" in text:
        return text
    head, blocks, cur = [], [], []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        if not s.startswith("#"):
            blocks.append((cur, s))
            cur = []
        elif not blocks and not cur and s.startswith(HEAD_TAGS):
            head.append(s)
        else:
            cur.append(s)
    if len(blocks) <= keep * 2:
        return text
    dropped = blocks[:-keep]
    carry, disc = {}, 0
    for tags, _ in dropped:
        for t in tags:
            if t.startswith(("#EXT-X-KEY", "#EXT-X-MAP")):
                carry[t[:10]] = t
            elif t == "#EXT-X-DISCONTINUITY":
                disc += 1
    first = blocks[-keep][0]
    out, seq, dseq = [], len(dropped), disc
    for h in head:
        if h.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            seq += int(h.split(":", 1)[1] or 0)
            continue
        if h.startswith("#EXT-X-DISCONTINUITY-SEQUENCE:"):
            dseq += int(h.split(":", 1)[1] or 0)
            continue
        if not h.startswith("#EXT-X-PLAYLIST-TYPE"):   # EVENT/VOD после обрезки неправда
            out.append(h)
    out.append(f"#EXT-X-MEDIA-SEQUENCE:{seq}")
    if dseq:
        out.append(f"#EXT-X-DISCONTINUITY-SEQUENCE:{dseq}")
    out += [t for k, t in carry.items() if not any(x.startswith(k) for x in first)]
    for tags, uri in blocks[-keep:]:
        out += tags + [uri]
    return "\n".join(out + cur) + "\n"


# Вариант тоже идёт через NAS (/wv): длинный хвост обрезаем, а только что скачанный (вместе с master)
# отдаём из памяти — плееру не ждать второго запроса к источнику.
V_CACHE = {}   # адрес варианта → (текст, срок)
V_WAIT = {}    # адрес → Event, пока его качают
V_LOCK = threading.Lock()


def wv_local(url, ua, local):
    return f"{local}/wv?u={quote(url, safe='')}" + (f"&ua={quote(ua, safe='')}" if ua else "")


def wv_put(url, body, final):
    text = trim_live(abs_playlist(body.decode("utf-8", "replace"), final))
    with V_LOCK:
        V_CACHE[url] = (text, time.time() + 2)
        if len(V_CACHE) > 200:
            V_CACHE.clear()
    return text


def wv_get(url, ua=None):
    with V_LOCK:
        hit = V_CACHE.get(url)
        if hit and hit[1] > time.time():
            return hit[0]
        ev = V_WAIT.get(url)
        mine = ev is None
        if mine:
            ev = V_WAIT[url] = threading.Event()
    if not mine:   # этот вариант уже качают (заранее, вместе с master) — ждём его
        ev.wait(10)
        with V_LOCK:
            hit = V_CACHE.get(url)
        if hit:
            return hit[0]
        raise LookupError("вариант не скачался")
    try:
        body, final, _ = fetch(url, ua or DEFAULT_UA, 4 << 20, 8)
        if b"#EXTM3U" not in body[:1024]:
            raise LookupError("не плейлист")
        return wv_put(url, body, final)
    finally:
        with V_LOCK:
            V_WAIT.pop(url, None)
        ev.set()


def wrap_master(url, ua=None, local=""):
    hit = W_CACHE.get(url)
    if hit and hit[1] > time.time():
        return hit[0]
    body, final, _ = fetch(url, ua or DEFAULT_UA, 4 << 20, 8)
    text = body.decode("utf-8", "replace")
    if "#EXTM3U" not in text:
        raise LookupError("не плейлист")
    if "#EXT-X-STREAM-INF" in text:
        master = abs_playlist(text, final, lambda u: wv_local(u, ua, local))
        # плеер начнёт с самого лёгкого (STARTBITRATE=LOWEST) — качаем его, пока плеер разбирает master
        bw = [(int(m[1]), urljoin(final, m[2].strip())) for m in
              re.finditer(r"#EXT-X-STREAM-INF:[^\n]*?BANDWIDTH=(\d+)[^\n]*\n([^#\n][^\n]*)", text)]
        if bw:
            threading.Thread(target=lambda: wv_get(min(bw)[1], ua), daemon=True).start()
    else:
        wv_put(final, body, final)
        master = f"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=3000000\n{wv_local(final, ua, local)}\n"
    W_CACHE[url] = (master, time.time() + 30)   # у dmi3y хост варианта меняется — держим недолго
    if len(W_CACHE) > 500:
        W_CACHE.clear()
    return master


# ---------- логотипы: копии на NAS ----------
# Логотипы в источниках — в основном iptvx.one и imgur, а их домашний провайдер режет. Копии лежат в
# out/logos/<sha1(адрес)[:16]>.png (часть залита с Mac через VPN); недостающие докачиваем сами, если сайт отвечает.

def logo_name(url):
    return hashlib.sha1(url.encode()).hexdigest()[:16] + ".png"


def logo_fetch(out, urls):
    def run():
        d = os.path.join(out, "logos")
        os.makedirs(d, exist_ok=True)
        got = 0
        for u in urls:
            p = os.path.join(d, logo_name(u))
            if os.path.exists(p) or os.path.exists(p + ".bad"):
                continue
            try:
                data = fetch(u, timeout=8)[0]
                if len(data) < 100:
                    raise ValueError("пусто")
                open(p, "wb").write(data)
                got += 1
            except Exception:
                open(p + ".bad", "w").close()   # недоступен — не пробуем каждый раз
        if got:
            log(f"логотипы: докачано {got}")
    threading.Thread(target=run, daemon=True).start()


# ---------- результаты диагностики на телевизоре ----------
# Плеер Samsung прогоняет все источники (out/tvtest.json) и шлёт /zap?stage=test&res=ok|stall|novideo|…&first=мс.
# Храним по исходному адресу источника (без обёрток /w и /kz/r) в out/tv_results.json и ставим для плеера
# первыми источники, проверенно быстрые на ТВ; не работающие на ТВ — последними.

TV_RESULTS = {}


def tv_key(url):
    p = urlsplit(url)
    if p.path in ("/w", "/kz/r"):
        return parse_qs(p.query).get("u", [url])[0]
    return url


def tv_load(out):
    global TV_RESULTS
    try:
        TV_RESULTS = json.load(open(os.path.join(out, "tv_results.json")))
    except Exception:
        TV_RESULTS = {}


def tv_record(out, q):
    url = q.get("url", [""])[0]
    if not url:
        return
    TV_RESULTS[tv_key(url)] = {"res": q.get("res", ["?"])[0], "first": int(q.get("first", ["0"])[0] or 0),
                               "video": q.get("video", [""])[0], "t": int(time.time()),
                               "via": "relay" if urlsplit(url).path == "/kz/r" else ""}
    tmp = os.path.join(out, "tv_results.json.tmp")
    with open(tmp, "w") as f:
        json.dump(TV_RESULTS, f)
    os.replace(tmp, os.path.join(out, "tv_results.json"))


def tv_rank(stream):
    r = TV_RESULTS.get(tv_key(stream["url"]))
    if not r:
        return (1, 0)
    return (0, r["first"]) if r["res"] == "ok" else (2, 0)


def tv_relay(url):
    """Источник, который у ТВ не стартует, а у сборщика работает (ТВ ходит через DNS роутера на другие узлы
    CDN, часть — через туннель; плюс https на Tizen 5.5), — целиком через NAS. Раз прошёл так — так и оставляем."""
    r = TV_RESULTS.get(url)
    return bool(r) and (r.get("via") == "relay" or r["res"].split("-")[0] in ("timeout", "error", "stall"))


def sm_local(url, local):
    m = SM_URL.match(url)
    return f"{local}/sm/{m[1]}/{m[2]}.m3u8" if m else None


# ---------- кинозалы smotret-online.tv ----------
# Ссылки на потоки подписаны и живут около часа, поэтому в плейлисте — адрес этого сервера:
# /kz/<slug>.m3u8 добывает свежую ссылку (страница → iframe плеера → m3u8) и проксирует поток
# с заголовками браузера: без них плеер cdntvmedia отвечает 403 (как и на IP датацентров).

# разделы сайта задают группы всего плейлиста; наши группы без канала на сайте — в ближайший раздел
SITE_GROUP = {"Эфирные": "Общественные", "Популярные": "Развлекательные", "Новости": "Новостные",
              "Спорт": "Спортивные", "Музыка": "Музыкальные", "Кино и сериалы": "Фильмы и сериалы",
              "Детские": "Детские", "Познавательные": "Познавательные", "Развлекательные": "Развлекательные",
              "Региональные": "Региональные"}
REGION_GROUP, OTHER_GROUP = "Региональные", "Другие"
KZ_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
         "(KHTML, like Gecko) Version/18.6 Safari/605.1.15")
KZ_IFRAME = re.compile(r'<iframe[^>]+src=["\']([^"\']+)["\']', re.I)
KZ_M3U8 = re.compile(r'https?:(?:\\?/){2}[^"\'\s<>]+?\.m3u8[^"\'\s<>]*', re.I)


def site_key(name):
    """Ключ канала сайта — как в group_channels: каноническое имя каталога или norm()."""
    main = ALIAS.get(norm(name))
    return main[2] if main else norm(name)


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


def kz_open(url, ref, timeout=15, ua=None):
    """Поток или сегмент: CDN ждёт Origin/Referer плеера, со страницы которого взята ссылка.
    Без ref — обычный источник, который ТВ сам не тянет: идём как сборщик, с его UA."""
    if not ref:
        h = {"User-Agent": ua or DEFAULT_UA}
        return urllib.request.urlopen(urllib.request.Request(url, headers=h), timeout=timeout)
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
        self.index = None   # адрес раздела сайта: страница канала рядом с ним, пока список не загружен

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


def kz_relay(url, ref, local, ua=""):
    return f"{local}/kz/r?u={quote(url, safe='')}&ref={quote(ref, safe='')}" + (f"&ua={quote(ua, safe='')}" if ua else "")


def kz_rewrite(text, base_url, ref, local, slug, ua=""):
    """Все адреса плейлиста — через нас. Варианты master-плейлиста ссылаются на /kz/<slug>.m3u8?v=N,
    чтобы при обновлении плейлиста подпись бралась свежая, а не та, что была при старте."""
    def wrap(u):
        return kz_relay(urljoin(base_url, u), ref, local, ua)
    text = trim_live(text)
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
        sm = SM_URL.match(link)
        if sm:   # плеер сайта ведёт на один вариант smotrim, а они пропадают (у «Соловьёв.Live» исчез _6) — отдаём master
            return send(sm_master(*sm.groups()).encode(), "application/vnd.apple.mpegurl")
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
            ref, ua = q.get("ref", [""])[0], q.get("ua", [""])[0]
            with kz_open(q["u"][0], ref, timeout=20, ua=ua) as r:
                ctype = r.headers.get("Content-Type", "")
                head = r.read(7)   # livetv.php отдаёт плейлисты без mpegurl и без .m3u8 в пути
                if head == b"#EXTM3U" or "mpegurl" in ctype.lower():
                    text = (head + r.read()).decode("utf-8", "replace")
                    return send(kz_rewrite(text, r.geturl(), ref, local, "", ua).encode(), "application/vnd.apple.mpegurl")
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
        self.kz = []                         # каналы сайта: [{"slug", "name", "logo", "page", "group", "ok"}]
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
            0 if r.get("vcodec") == "mpeg2" else 1,   # MPEG-2: на Samsung только звук, на приставках — как повезёт
            0 if r["ms"] > START_SLOW_MS else 1,      # быстро ли стартует: переключение каналов важнее разрешения
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
        """Каналы всех разделов сайта (раздел = группа) и проверка, что у каждого добывается живой плейлист."""
        sections = self.cfg.get("site_sections", [])
        if not sections:
            self.kz = []
            return
        kz, seen = [], set()
        for url, grp in sections:
            try:
                got = kz_channels(url)
            except Exception as ex:
                log(f"сайт: раздел {grp} недоступен ({ex!r}), оставляю прошлый")
                got = [dict(c) for c in self.kz if c["group"] == grp]
            for c in got:
                if c["slug"] not in seen:       # канал в двух разделах — в первом
                    seen.add(c["slug"])
                    kz.append(dict(c, group=grp))
        with KZ.lock:
            KZ.pages = {c["slug"]: c["page"] for c in kz}

        def check(c):
            try:
                link, ref = KZ.get(c["slug"])
                if SM_URL.match(link):
                    return "#EXT-X-STREAM-INF" in sm_master(*SM_URL.match(link).groups())
                with kz_open(link, ref) as r:
                    return b"#EXTM3U" in r.read(4096)
            except Exception:
                KZ.drop(c["slug"])
                return False
        with cf.ThreadPoolExecutor(min(self.cfg.get("workers", 12), 6)) as ex:
            for c, ok in zip(kz, ex.map(check, kz)):
                c["ok"] = ok
        self.kz = kz
        log(f"сайт: живых {sum(c['ok'] for c in kz)} из {len(kz)}")

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
        local = cfg.get("kz_base") or f"http://127.0.0.1:{cfg.get('port', 8090)}"
        # каналы сайта: совпавшие с нашими дают им раздел сайта и запасной поток через /kz/, остальные — новыми каналами
        site, spos = {}, {}
        for i, sc in enumerate(self.kz):
            k = site_key(sc["name"])
            if k not in site:
                site[k], spos[k] = sc, i
        any_ok = any(sc["ok"] for sc in self.kz)   # не прошёл ни один — сайт лёг на время проверки, адреса-то наши
        sections = [g for _, g in cfg.get("site_sections", [])]

        def group(c):
            sc = site.get(c["key"])
            if sc:
                return sc["group"]
            return SITE_GROUP.get(c["group"], OTHER_GROUP) if sections else c["group"]

        order = sections + [REGION_GROUP, OTHER_GROUP] if sections else MAIN_GROUPS
        gorder = {g: i for i, g in enumerate(order)}
        units = [dict(c, group=group(c)) for c in self.chans.values()]
        units += [{"key": k, "name": sc["name"], "group": sc["group"], "main": None, "cand": [], "tvg": {"logo": sc["logo"]}}
                  for k, sc in site.items() if k not in self.chans]

        def key(c):   # раздел; внутри — порядок сайта, затем основные по каталогу, затем по алфавиту
            return (gorder.get(c["group"], 99), c["group"], spos.get(c["key"], 10 ** 6),
                    c["main"][0] if c["main"] else 10 ** 6, c["name"].lower())

        report, main_lines, all_lines, player = [], [], [], []
        sm_keys, logos = set(), set()
        for c in sorted(units, key=key):
            url = self.chosen.get(c["key"])
            best = next((e for e in c["cand"] if e["url"] == url), None)
            r = self.last.get(url, {}) if best else {}
            ok = bool(r.get("ok"))
            sc = site.get(c["key"])
            # у наших каналов поток сайта — всегда последним запасным (проверка могла не пройти случайно, а он
            # бывает единственным рабочим, как у «Мира»); канал только с сайта — если поток добылся или сайт лёг целиком
            surl = f"{local}/kz/{sc['slug']}.m3u8" if sc and (sc["ok"] or not any_ok or c["cand"]) else None
            report.append({
                "channel": c["name"], "group": c["group"], "main": bool(c["main"]), "ok": ok or bool(surl),
                "alive": sum(1 for e in c["cand"] if self.last.get(e["url"], {}).get("ok")) + bool(surl),
                "total": len(c["cand"]) + bool(sc),
                "url": url if ok else surl, "src": best["src"] if ok else ("smotret-online" if surl else None),
                "height": r.get("height", 0), "need": r.get("need", 0), "got": r.get("got", 0), "ms": r.get("ms", 0),
                "rate": self.rate(url) if ok else (1 if surl else 0),
                "errors": sorted({self.last.get(e["url"], {}).get("err", "") for e in c["cand"]} - {""}),
            })
            if not ok and not surl:
                continue
            attrs = (f'tvg-id="{c["tvg"].get("id", "")}" tvg-name="{c["name"]}" tvg-logo="{c["tvg"].get("logo", "") or (sc or {}).get("logo", "")}" '
                     f'group-title="{c["group"]}"')
            lines = [f"#EXTINF:-1 {attrs},{c['name']}"]
            if ok and best["ua"]:
                lines.append(f"#EXTVLCOPT:http-user-agent={best['ua']}")
            if ok and best["ref"]:
                lines.append(f"#EXTVLCOPT:http-referrer={best['ref']}")
            lines.append(url if ok else surl)
            all_lines += lines
            # для своего плеера: выбранный поток первым, затем живые запасные по убыванию качества, сайт — последним
            alive = sorted((e for e in c["cand"] if self.last.get(e["url"], {}).get("ok")),
                           key=lambda e: (e["url"] == url, self.score(e, url)), reverse=True)[:cfg.get("player_streams", 10)]
            streams = []
            for e in alive:
                sm = sm_local(e["url"], local)
                if sm:   # сначала master со всеми качествами (старт с лёгкого), исходный вариант — запасным
                    streams.append({"url": sm, "ua": "", "ref": "", "hls": True})
                    sm_keys.add(SM_URL.match(e["url"]).groups())
                hls = bool(self.last[e["url"]].get("hls"))
                u = e["url"]
                if hls and RELAY_HOSTS.search(urlsplit(u).hostname or ""):
                    u = kz_relay(u, kz_origin(u) + "/", local)
                elif hls and not e["ref"] and tv_relay(u):
                    u = kz_relay(u, "", local, e["ua"] or "")
                elif hls and not e["ref"]:   # первый плейлист — с NAS: так AVPlay не виснет на подготовке
                    u = f"{local}/w?u={quote(u, safe='')}" + (f"&ua={quote(e['ua'], safe='')}" if e["ua"] else "")
                streams.append({"url": u, "ua": e["ua"] or "", "ref": e["ref"] or "", "hls": hls,
                                "v": self.last[e["url"]].get("vcodec", "")})
            if surl:
                streams.append({"url": surl, "ua": "", "ref": "", "hls": True})
            if TV_RESULTS:   # проверено на телевизоре: быстрые первыми, неработающие последними (sort устойчив)
                streams.sort(key=tv_rank)
            logo = c["tvg"].get("logo", "") or (sc or {}).get("logo", "")
            if logo:
                logos.add(logo)
                if os.path.exists(os.path.join(out, "logos", logo_name(logo))):
                    logo = f"{local}/logos/{logo_name(logo)}"
            player.append({"n": len(player) + 1, "name": c["name"], "group": c["group"], "main": bool(c["main"]),
                           "tvg": c["tvg"].get("id", ""), "logo": logo, "streams": streams})
            if c["main"]:
                main_lines += lines
        sm_prewarm(sorted(sm_keys))
        logo_fetch(out, sorted(logos))
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
# Раз в epg_every_min качаем XMLTV из epg_sources и оставляем только каналы из channels.json на окно
# [-3 ч, +48 ч] → epg.json {"updated", "ch": {имя канала: [[начало, конец, название], …]}}.
# Источники по порядку: канал берёт программу из первого, где она нашлась. iptvx.one (его id в tvg-id плейлистов)
# стоит на Cloudflare, и домашний провайдер рвёт к нему TLS — тогда выручают teleguide.info, programtv.ru и
# ru.epg.one по http (сопоставление по названию и синонимам каталога; у it999 часть каналов бывает устаревшей).
# Разбираем построчно байтами, а не ElementTree: файлы на 200–400 МБ, на приставке это в разы быстрее и почти без памяти.

EPG_CH = re.compile(rb'<channel id="([^"]+)"')
EPG_NAME = re.compile(rb"<display-name[^>]*>([^<]+)</display-name>")
EPG_PROG = re.compile(rb'<programme start="(\d{14}) ?([+-]\d{4})?" stop="(\d{14}) ?([+-]\d{4})?" channel="([^"]+)"')
EPG_TITLE = re.compile(rb"<title[^>]*>([^<]*)</title>")


def name_key(name):
    """Строже norm(): скобки («12 канал (Омск)») не выбрасываем, иначе регионы склеятся."""
    s = re.sub(r"\b(hd|fhd|uhd|sd|orig)\b", "", name.lower().replace("ё", "е"))
    return re.sub(r"[^0-9a-zа-я]+", "", s)


def xmltv_ts(t, tz):
    ts = calendar.timegm((int(t[:4]), int(t[4:6]), int(t[6:8]), int(t[8:10]), int(t[10:12]), int(t[12:14]), 0, 0, 0))
    if tz:
        off = int(tz[1:3]) * 3600 + int(tz[3:5]) * 60
        ts -= off if tz[:1] == b"+" else -off
    return ts


def parse_epg(lines, chans, lo, hi):
    """XMLTV построчно; элемент может занимать одну строку (iptvx.one) или несколько (it999/epg.one)."""
    ids, by_name = {}, {}
    for c in chans:
        if c["tvg"]:
            ids.setdefault(c["tvg"], []).append(c["name"])
        by_name.setdefault(name_key(c["name"]), []).append(c["name"])
    have = {c["name"] for c in chans}
    for _, canon, al in MAIN:                # синонимы каталога: «Культура» → «Россия К», «Матч!» → «Матч ТВ»
        if canon in have:
            for a in al:
                k = by_name.setdefault(name_key(a), [])
                if canon not in k:
                    k.append(canon)
    # имя канала → кандидаты в программе по приоритету: свой tvg-id, затем основное название, затем запасные.
    # «Россия 1» бывает запасным названием у регионального дубля без передач — поэтому берём первого непустого.
    cands = {}
    want = None                              # id программы → имена каналов плейлиста
    got, ch, cur = {}, None, None            # (имя, id) → передачи
    for line in lines:
        s = line.lstrip()
        if s.startswith(b"<programme"):
            if want is None:                 # в XMLTV все <channel> идут до передач — сопоставляем один раз
                want = {}
                for name, cs in cands.items():
                    for _, cid in cs:
                        if name not in want.setdefault(cid, []):
                            want[cid].append(name)
            cur = None
            m = EPG_PROG.match(s)
            if m and m.group(5) in want:
                a, b = xmltv_ts(m.group(1), m.group(2)), xmltv_ts(m.group(3), m.group(4))
                if b > lo and a < hi:
                    cur = (a, b, want[m.group(5)], m.group(5))
        elif s.startswith(b"<channel"):
            m = EPG_CH.match(s)
            ch = (m.group(1), []) if m else None
        if ch is not None:
            ch[1].extend(EPG_NAME.findall(line))
            if b"</channel>" in line:
                cid = ch[0]
                for name in ids.get(cid.decode(), []):
                    cands.setdefault(name, []).append((0, cid))
                for i, dn in enumerate(ch[1]):
                    for name in by_name.get(name_key(html.unescape(dn.decode("utf-8", "replace"))), []):
                        cands.setdefault(name, []).append((1 if i == 0 else 2, cid))
                ch = None
        if cur is not None:
            t = EPG_TITLE.search(line)
            if t or b"</programme>" in line:
                title = html.unescape(t.group(1).decode("utf-8", "replace")) if t else ""
                title = re.sub(r"\s*[(\[]\d{1,2}\+[)\]]\s*$", "", title.strip())   # «Время покажет (16+)»
                for name in cur[2]:
                    got.setdefault((name, cur[3]), []).append([cur[0], cur[1], title])
                cur = None
    progs = {}
    for name, cs in cands.items():
        for _, cid in sorted(cs, key=lambda x: x[0]):   # sorted устойчив: внутри приоритета — порядок файла
            if got.get((name, cid)):
                progs[name] = got[(name, cid)]
                break
    return progs


def build_epg(cfg):
    out = cfg["out_dir"]
    chans = json.load(open(os.path.join(out, "channels.json"), encoding="utf-8"))["channels"]
    lo, hi = time.time() - 3 * 3600, time.time() + 48 * 3600
    progs = {}
    for url in cfg.get("epg_sources") or [cfg["epg"]]:
        t0 = time.time()
        try:
            req = urllib.request.Request(url, headers={"User-Agent": DEFAULT_UA})
            with urllib.request.urlopen(req, timeout=60) as r, gzip.GzipFile(fileobj=r) as f:
                got = parse_epg(f, chans, lo, hi)
        except Exception as ex:
            log(f"программа {url}: {ex!r}")
            continue
        new = {k: v for k, v in got.items() if k not in progs}
        progs.update(new)
        log(f"программа {url}: {len(got)} каналов, новых {len(new)}, {int(time.time() - t0)} с")
    for v in progs.values():
        v.sort()
    if not progs:
        raise ValueError("ни один источник программы не дал наших каналов")
    tmp = os.path.join(out, "epg.json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"updated": int(time.time()), "ch": progs}, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, os.path.join(out, "epg.json"))
    log(f"программа: {len(progs)} из {len(chans)} каналов, {sum(map(len, progs.values()))} передач")


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
    KZ.index = (cfg.get("site_sections") or [[None]])[0][0]
    tv_load(out)

    class H(SimpleHTTPRequestHandler):
        def __init__(self, *a, **k):
            super().__init__(*a, directory=out, **k)

        def do_GET(self):
            if self.path.startswith("/kz/"):
                return kz_handle(self)
            if self.path == "/log":   # хвост журнала — плеер прикладывает его к «Отправить журнал»
                try:
                    with open("agg.log", "rb") as f:
                        f.seek(max(0, os.path.getsize("agg.log") - 600_000))
                        body = f.read()
                except OSError:
                    body = b""
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                return self.wfile.write(body)
            if self.path.startswith(("/w?", "/wv?")):
                q = parse_qs(urlsplit(self.path).query)
                ua = q.get("ua", [None])[0]
                try:
                    if self.path.startswith("/wv?"):
                        body = wv_get(q["u"][0], ua).encode()
                    else:
                        body = wrap_master(q["u"][0], ua, f"http://{self.headers.get('Host')}").encode()
                except Exception as ex:
                    log(f"обёртка {q.get('u', ['?'])[0][:80]}: {ex!r}")
                    return self.send_error(502)
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                return self.wfile.write(body)
            if self.path.startswith("/zap?"):   # замеры переключения от плеера телевизора — в журнал
                q = parse_qs(urlsplit(self.path).query)
                if q.get("stage") == ["test"] and q.get("id", [""])[0].startswith("full"):
                    tv_record(out, q)
                log("zap " + " ".join(f"{k}={v[0][:300 if k == 'steps' else 90]}" for k, v in q.items()))
                self.send_response(204)
                self.end_headers()
                return
            m = re.fullmatch(r"/sm/(hls2?)/([\w-]+)\.m3u8", self.path)
            if m:
                try:
                    body = sm_master(m[1], m[2]).encode()
                except Exception as ex:
                    log(f"smotrim {self.path}: {ex!r}")
                    return self.send_error(502)
                self.send_response(200)
                self.send_header("Content-Type", "application/vnd.apple.mpegurl")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                return self.wfile.write(body)
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
    assert name_key("12 канал (Омск) HD") == "12каналомск" == name_key("12 канал HD (Омск)")
    one = [{"name": "Первый канал", "tvg": "pervy"}, {"name": "Россия 1", "tvg": ""}]
    xml = ('<channel id="pervy"><display-name>Первый</display-name></channel>\n<channel id="7">\n'
           '<display-name lang="ru">Россия 1 HD</display-name>\n</channel>\n'
           '<programme start="20261022120000 +0300" stop="20261022130000 +0300" channel="pervy"><title>Новости</title></programme>\n'
           '<programme start="20261022120000 +0300" stop="20261022130000 +0300" channel="7">\n<title lang="ru">Вести</title>\n</programme>\n').encode()
    assert parse_epg(xml.splitlines(True), one, 0, 2e9) == {"Первый канал": [[1792659600, 1792663200, "Новости"]],
                                                           "Россия 1": [[1792659600, 1792663200, "Вести"]]}
    seg = bytes([0x47, 0x40, 0x00, 0x10, 0x00, 0x00, 0xb0, 0x0d, 0x00, 0x01, 0xc1, 0x00, 0x00, 0x00, 0x01, 0xe1, 0x00]) + bytes(171)
    seg += bytes([0x47, 0x41, 0x00, 0x10, 0x00, 0x02, 0xb0, 0x12, 0x00, 0x01, 0xc1, 0x00, 0x00, 0xe1, 0x01, 0xf0, 0x00,
                  0x02, 0xe1, 0x01, 0xf0, 0x00]) + bytes(166)
    assert ts_video_codec(seg) == "mpeg2", ts_video_codec(seg)
    assert sm_local("https://stream.smotrim.ru/hls2/russia_hd/playlist_6.m3u8", "http://l") == "http://l/sm/hls2/russia_hd.m3u8"
    dvr = ("#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXT-X-MEDIA-SEQUENCE:100\n#EXT-X-PLAYLIST-TYPE:EVENT\n"
           '#EXT-X-KEY:METHOD=AES-128,URI="k1"\n' + "".join(
               ("#EXT-X-DISCONTINUITY\n" if i == 3 else "") + f"#EXTINF:6,\ns{i}.ts\n" for i in range(30)))
    t = trim_live(dvr)
    assert t.count("#EXTINF") == LIVE_KEEP and "s29.ts" in t and "s21.ts" not in t and "s22.ts" in t
    assert "#EXT-X-MEDIA-SEQUENCE:122" in t and "#EXT-X-DISCONTINUITY-SEQUENCE:1" in t
    assert 'URI="k1"' in t and "PLAYLIST-TYPE" not in t and t.index("KEY") < t.index("s22.ts")
    assert trim_live(dvr + "#EXT-X-ENDLIST\n") == dvr + "#EXT-X-ENDLIST\n"
    assert "/wv?u=https%3A%2F%2Fc%2Fhi.m3u8" in abs_playlist(
        "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nhi.m3u8\n", "https://c/m.m3u8", lambda u: wv_local(u, "", "http://l"))
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
