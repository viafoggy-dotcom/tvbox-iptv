#!/usr/bin/env python3
"""Сборщик плейлиста российских каналов из m3u.su с постоянной проверкой потоков.

  python3 agg.py config.json           — сервис: проверки по расписанию + HTTP-сервер
  python3 agg.py config.json --once    — одна полная проверка, файлы в out_dir

Уровни проверки (чтобы не гонять гигабайты на приставке):
  main_every_min  — текущие потоки основных каналов; упавший сразу меняется на лучший запасной
  all_every_min   — то же для всех каналов
  full_every_min  — заново скачать источники, проверить все варианты, переизбрать лучших

Отдаёт: /playlist.m3u (основные, затем все остальные), /main.m3u, /status, /status.json.
"""
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
from urllib.parse import urljoin

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
    entries = []
    for s in cfg["sources"]:
        url = s if s.startswith("http") else f"https://m3u.su/{s}"
        try:
            body, real, _ = fetch(url, timeout=30)
            e = parse_m3u(body.decode("utf-8", "replace"), s)
            entries += e
            log(f"источник {s}: {len(e)} записей")
        except Exception as ex:
            log(f"источник {s}: ошибка {ex}")
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
        self.lock = threading.Lock()

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

        report, main_lines, all_lines = [], [], []
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
            if c["main"]:
                main_lines += lines
        for fn, ls in (("main.m3u", main_lines), ("playlist.m3u", all_lines)):
            tmp = os.path.join(out, fn + ".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                f.write("\n".join([hdr] + ls) + "\n")
            os.replace(tmp, os.path.join(out, fn))
        status = {"updated": time.strftime("%Y-%m-%d %H:%M:%S"), "took_s": took, "channels": report}
        json.dump(status, open(os.path.join(out, "status.json"), "w"), ensure_ascii=False, indent=1)
        with open(os.path.join(out, "status.html"), "w", encoding="utf-8") as f:
            f.write(status_html(status))
        self.save()
        m = [x for x in report if x["main"]]
        log(f"записано: основные {sum(x['ok'] for x in m)}/{len(m)}, всего {sum(x['ok'] for x in report)}/{len(report)}")

    def step(self, level):
        t0 = time.time()
        with self.lock:
            if level == "full" or not self.chans:
                self.full()
            else:
                self.recheck(main_only=(level == "main"))
            self.write(int(time.time() - t0))


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


# ---------- сервис ----------

def serve(cfg):
    out = os.path.abspath(cfg["out_dir"])

    class H(SimpleHTTPRequestHandler):
        def __init__(self, *a, **k):
            super().__init__(*a, directory=out, **k)

        def do_GET(self):
            if self.path in ("/", "/status"):
                self.path = "/status.html"
            super().do_GET()

        def guess_type(self, path):
            return "audio/x-mpegurl; charset=utf-8" if str(path).endswith(".m3u") else super().guess_type(path)

        def log_message(self, *a):
            pass

    srv = ThreadingHTTPServer((cfg.get("listen", "0.0.0.0"), cfg.get("port", 8090)), H)
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
