#!/usr/bin/env python3
"""
VPN Gate SSTP 节点检测流水线 (精简版)
=====================================
流程:
  1. 获取 VPN Gate 原始节点
  2. 只保留带 TCP 入口的 SSTP 节点
  3. 去重
  4. 并发调用检测 Worker
  5. 生成 public/data.json + public/index.html + public/nodes.txt
"""

import base64
import csv
import io
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import quote

import requests

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------
REPO_DIR = os.path.dirname(os.path.abspath(__file__))

VPNGATE_API = os.environ.get("VPNGATE_API", "http://www.vpngate.net/api/iphone/")
VPNGATE_MIRROR = os.environ.get(
    "VPNGATE_MIRROR",
    "https://raw.githubusercontent.com/fdciabdul/Vpngate-Scraper-API/main/json/data.json",
)
WORKER_CHECK_URL = os.environ.get("CHECK_WORKER", "https://你的域名/check?sstp=vpn:vpn@")
CONCURRENCY = max(1, int(os.environ.get("CHECK_CONCURRENCY", "32")))
CHECK_TIMEOUT = float(os.environ.get("CHECK_TIMEOUT", "90"))
MAX_CHECK_NODES = int(os.environ.get("MAX_CHECK_NODES", "0"))
HTTP_TIMEOUT = int(os.environ.get("HTTP_TIMEOUT", "60"))
PUBLIC_DIR = os.environ.get("PUBLIC_DIR", os.path.join(REPO_DIR, "public"))
TEMPLATE_HTML = os.path.join(REPO_DIR, "web", "index.html")

DATA_CENTER_ORG_KEYWORDS = [
    "GOOGLE", "AMAZON", "AWS", "MICROSOFT", "OVH", "HETZNER", "DIGITALOCEAN",
    "AKAMAI", "CLOUDFLARE", "FASTLY", "RACKSPACE", "EQUINIX", "LINODE", "VULTR",
    "HURRICANE", "TENCENT", "ALIBABA", "ALIYUN", "LEASWEB",
]
RESIDENTIAL_ORG_KEYWORDS = [
    "NTT EAST", "NTT WEST", "NTT COMMUNICATIONS", "NTT BROADBAND", "KDDI", "DOCOMO",
    "SOFTBANK", "AU COMMUNICATIONS", "J:COM", "JCOM", "OCN", "BIGLOBE",
    "IIJ", "SEIKO", "CLEVER-NET", "AT&T", "COMCAST", "XFINITY", "VERIZON",
    "TELUS", "ROGERS", "BELL CANADA", "VODAFONE", "ORANGE", "DEUTSCHE TELEKOM",
    "BREEZE", "TIM S.P.A", "LIBERO", "FASTWEB", "FREE FRANCE", "BT OPEN",
]

COUNTRY_ZH = {
    "JP": "日本", "KR": "韩国", "US": "美国", "CA": "加拿大", "RU": "俄罗斯",
    "RO": "罗马尼亚", "TH": "泰国", "VN": "越南", "DE": "德国", "FR": "法国",
    "GB": "英国", "UK": "英国", "SG": "新加坡", "TW": "台湾", "HK": "香港",
    "CN": "中国", "AU": "澳大利亚", "NL": "荷兰", "SE": "瑞典", "CH": "瑞士",
    "IT": "意大利", "ES": "西班牙", "PL": "波兰", "IN": "印度", "BR": "巴西",
    "MX": "墨西哥", "ID": "印度尼西亚", "MY": "马来西亚", "PH": "菲律宾",
    "TR": "土耳其", "UA": "乌克兰", "CZ": "捷克", "GR": "希腊", "PT": "葡萄牙",
    "FI": "芬兰", "NO": "挪威", "DK": "丹麦", "IE": "爱尔兰", "BE": "比利时",
    "AT": "奥地利", "HU": "匈牙利", "AR": "阿根廷", "CL": "智利", "CO": "哥伦比亚",
    "NZ": "新西兰", "ZA": "南非", "IL": "以色列", "AE": "阿联酋", "SA": "沙特",
    "EG": "埃及", "HR": "克罗地亚", "BY": "白俄罗斯", "GD": "格林纳达",
    "LV": "拉脱维亚", "EE": "爱沙尼亚", "LT": "立陶宛", "SK": "斯洛伐克",
    "SI": "斯洛文尼亚", "BG": "保加利亚", "RS": "塞尔维亚", "GE": "格鲁吉亚",
    "MD": "摩尔多瓦", "AM": "亚美尼亚", "KZ": "哈萨克斯坦", "UZ": "乌兹别克斯坦",
    "MN": "蒙古", "NP": "尼泊尔", "LK": "斯里兰卡", "MM": "缅甸",
}

# ---------------------------------------------------------------------------
# 日志
# ---------------------------------------------------------------------------
_section = None

def log(section, msg=""):
    global _section
    if section != _section:
        print(f"========== {section} ==========")
        _section = section
    if msg:
        print(msg, flush=True)

def die(msg):
    log("FATAL", f"[失败] {msg}")
    sys.exit(1)

# ---------------------------------------------------------------------------
# 数据抓取
# ---------------------------------------------------------------------------
def fetch_vpngate():
    try:
        log("VPN GATE", f"获取官方 API: {VPNGATE_API}")
        resp = requests.get(VPNGATE_API, timeout=HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (compatible; gate-checker)"})
        resp.raise_for_status()
        rows = parse_csv(resp.text)
        if rows:
            log("VPN GATE", f"主源(官方 API) 获取到 {len(rows)} 个原始节点")
            return rows, "vpngate.net/api/iphone"
        raise RuntimeError("官方 API 返回 0 行数据")
    except Exception as exc:
        log("VPN GATE", f"官方 API 获取失败: {exc}")

    try:
        log("VPN GATE", f"回退镜像: {VPNGATE_MIRROR}")
        resp = requests.get(VPNGATE_MIRROR, timeout=HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        rows = parse_mirror_json(resp.json())
        if rows:
            log("VPN GATE", f"回退源(镜像) 获取到 {len(rows)} 个原始节点")
            return rows, "github-mirror"
    except Exception as exc:
        log("VPN GATE", f"回退镜像也失败: {exc}")
    die("VPN Gate 官方 API 与回退镜像均不可用, 数据源完全失败")

def parse_csv(text):
    lines = [ln for ln in text.splitlines() if ln.strip()]
    header_idx = None
    for i, ln in enumerate(lines):
        if ln.lstrip("#").startswith("HostName"):
            header_idx = i
            break
    if header_idx is None:
        raise RuntimeError("找不到 CSV 表头行 (HostName)")

    header = lines[header_idx].lstrip("#").split(",")
    data_lines = lines[header_idx + 1:]
    idx = {}
    for col in ("hostname", "ip", "countrylong", "countryshort", "openvpn_configdata_base64"):
        for i, h in enumerate(header):
            if h.strip().lstrip("*").lower() == col:
                idx[col] = i
                break
    if "openvpn_configdata_base64" not in idx:
        for i, h in enumerate(header):
            if "base64" in h.lower():
                idx["openvpn_configdata_base64"] = i
                break
    pos = {"hostname": idx.get("hostname", 0), "ip": idx.get("ip", 1), "countrylong": idx.get("countrylong", 5), "countryshort": idx.get("countryshort", 6), "openvpn_configdata_base64": idx.get("openvpn_configdata_base64", len(header) - 1)}

    rows = []
    for ln in data_lines:
        fields = next(csv.reader(io.StringIO(ln)))
        if len(fields) < 7: continue
        host = fields[pos["hostname"]].strip()
        ip = fields[pos["ip"]].strip()
        if not host or not ip: continue
        rows.append({"host": host, "ip": ip, "country_long": fields[pos["countrylong"]].strip(), "country_short": fields[pos["countryshort"]].strip(), "config_b64": fields[pos["openvpn_configdata_base64"]].strip()})
    return rows

def parse_mirror_json(data):
    servers = []
    items = data if isinstance(data, list) else [data]
    for item in items:
        if isinstance(item, dict) and isinstance(item.get("servers"), list):
            servers.extend(item["servers"])
        elif isinstance(item, dict):
            servers.append(item)
    rows = []
    for s in servers:
        host = str(s.get("hostname") or s.get("host") or "").strip()
        ip = str(s.get("ip") or "").strip()
        if not host or not ip: continue
        rows.append({"host": host, "ip": ip, "country_long": str(s.get("countrylong") or s.get("country_long") or s.get("country") or "").strip(), "country_short": str(s.get("countryshort") or s.get("country_short") or "").strip(), "config_b64": str(s.get("openvpn_configdata_base64") or s.get("config_b64") or "").strip()})
    return rows

# ---------------------------------------------------------------------------
# 筛选 SSTP 节点
# ---------------------------------------------------------------------------
_PROTO_TCP_RE = re.compile(r"^proto\s+(tcp|tcp4|tcp6)\b", re.M)
_REMOTE_RE = re.compile(r"^remote\s+\S+\s+(\d+)", re.M)

def to_sstp_nodes(rows):
    nodes = []
    for r in rows:
        cfg = ""
        if r["config_b64"]:
            try:
                cfg = base64.b64decode(r["config_b64"], validate=False).decode("utf-8", "replace")
            except Exception:
                cfg = ""
        if not _PROTO_TCP_RE.search(cfg): continue
        m = _REMOTE_RE.search(cfg)
        if not m: continue
        port = int(m.group(1))
        if not (1 <= port <= 65535): continue
        host = r["host"]
        if not host.endswith(".opengw.net"):
            host = f"{host}.opengw.net"
        nodes.append({"host": host, "port": port, "ip": r["ip"], "country": r["country_long"], "country_code": r["country_short"]})
    return nodes

def dedupe(nodes):
    seen = set()
    out = []
    for n in nodes:
        key = (n["host"].lower(), n["port"], "sstp")
        if key in seen: continue
        seen.add(key)
        out.append(n)
    return out

# ---------------------------------------------------------------------------
# 多源 SOCKS5 代理抓取 (与 SSTP 池互补, 走同一 Worker 的 /check?socks5= 接口)
# ---------------------------------------------------------------------------
# 源格式: 一行一个 IP:PORT (可带 socks5:// 前缀和注释行)
SOCKS5_SOURCES = [
    ("proxifly",   "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/socks5/data.txt"),
    ("monosans",   "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/socks5.txt"),
    ("roosterkid", "https://raw.githubusercontent.com/roosterkid/openproxylist/main/SOCKS5.txt"),
    ("hookzof",    "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt"),
]
# 带元数据(国家/ASN)的精品源: monosans 的 json, 1024 个精选代理
SOCKS5_META_SOURCES = [
    ("monosans-meta", "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies.json"),
]
# 可配置: 只在环境变量 SOCKS5_SOURCES 非空时启用; 设为 "off" 可关闭
_SOCKS5_ENV = os.environ.get("SOCKS5_SOURCES", "").strip()
SOCKS5_ENABLED = _SOCKS5_ENV.lower() not in ("off", "0", "false", "no")
SOCKS5_MAX = int(os.environ.get("SOCKS5_MAX", "400"))  # 每轮最多测多少个(控制时间)
# 允许的国家白名单(逗号分隔 ISO 码); 留空=不过滤
SOCKS5_COUNTRIES = [c.strip().upper() for c in os.environ.get("SOCKS5_COUNTRIES", "").split(",") if c.strip()]
# 纯净模式(默认开): nodes.txt 只输出住宅节点, 剔除机房 IP
PURE_MODE = os.environ.get("PURE_MODE", "on").strip().lower() not in ("off", "0", "false", "no")
# SOCKS5 仅住宅(默认开): SOCKS5 池里剔除机房代理
SOCKS5_RESIDENTIAL_ONLY = os.environ.get("SOCKS5_RESIDENTIAL_ONLY", "on").strip().lower() not in ("off", "0", "false", "no")

_IPPORT_RE = re.compile(r"\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}):(\d{2,5})\b")

def _valid_ipv4(ip):
    parts = ip.split(".")
    if len(parts) != 4: return False
    try:
        return all(0 <= int(p) <= 255 for p in parts)
    except Exception:
        return False

def _bad_ip(ip):
    if ip.startswith(("0.", "10.", "127.", "169.254.", "192.168.", "224.", "255.")): return True
    if ip.startswith(("172.16.", "172.17.", "172.18.", "172.19.", "172.2", "172.30.", "172.31.")): return True
    return False

def fetch_socks5_meta():
    """从带元数据的精品源(monosans json)抓取 SOCKS5, 返回 [{proxy, country_code, org, geo}]"""
    out = []
    if not SOCKS5_ENABLED:
        return out
    for name, url in SOCKS5_META_SOURCES:
        try:
            resp = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (compatible; gate-checker)"})
            if resp.status_code != 200:
                log("SOCKS5", f"{name}: HTTP {resp.status_code} 跳过")
                continue
            arr = resp.json()
            n = 0
            for item in arr:
                proto = str(item.get("protocol") or "").lower()
                if proto != "socks5":
                    continue
                host = str(item.get("host") or "").strip()
                port = item.get("port")
                if not _valid_ipv4(host) or _bad_ip(host):
                    continue
                try:
                    port = int(port)
                except Exception:
                    continue
                if not (1 <= port <= 65535):
                    continue
                geo = item.get("geolocation") or {}
                cc = str((geo.get("country") or {}).get("iso_code") or "").upper()
                city = (((geo.get("city") or {}).get("names") or {}).get("en") or "")
                asn = item.get("asn") or {}
                org = str(asn.get("autonomous_system_organization") or "")
                if SOCKS5_COUNTRIES and cc not in SOCKS5_COUNTRIES:
                    continue
                out.append({"proxy": f"{host}:{port}", "country_code": cc, "city": city, "org": org})
                n += 1
            log("SOCKS5", f"{name}: 元数据源命中 {n} 个")
        except Exception as exc:
            log("SOCKS5", f"{name}: 抓取失败 {type(exc).__name__}: {exc}")
    return out

def fetch_socks5_sources():
    """优先用元数据精品源; 不足时再从公开列表补充。返回 [{proxy, country_code, city, org}]"""
    if not SOCKS5_ENABLED:
        log("SOCKS5", "已通过 SOCKS5_SOURCES=off 关闭")
        return []
    items = []
    seen = set()
    # 1) 精品源
    for m in fetch_socks5_meta():
        if m["proxy"] in seen:
            continue
        seen.add(m["proxy"])
        items.append(m)
    # 2) 公开列表补充(仅当精品源不足 SOCKS5_MAX)
    if len(items) < SOCKS5_MAX:
        for name, url in SOCKS5_SOURCES:
            if len(items) >= SOCKS5_MAX:
                break
            try:
                resp = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (compatible; gate-checker)"})
                if resp.status_code != 200:
                    log("SOCKS5", f"{name}: HTTP {resp.status_code} 跳过")
                    continue
                n_before = len(items)
                for m in _IPPORT_RE.finditer(resp.text):
                    if len(items) >= SOCKS5_MAX:
                        break
                    ip, port = m.group(1), int(m.group(2))
                    if not _valid_ipv4(ip) or _bad_ip(ip): continue
                    if not (1 <= port <= 65535): continue
                    key = f"{ip}:{port}"
                    if key in seen: continue
                    seen.add(key)
                    items.append({"proxy": key, "country_code": "", "city": "", "org": ""})
                log("SOCKS5", f"{name}: 新增 {len(items) - n_before} (累计 {len(items)})")
            except Exception as exc:
                log("SOCKS5", f"{name}: 抓取失败 {type(exc).__name__}: {exc}")
    log("SOCKS5", f"多源合计 {len(items)} 个 SOCKS5 代理 (含元数据 {sum(1 for i in items if i['country_code'])} 个)")
    if SOCKS5_MAX > 0 and len(items) > SOCKS5_MAX:
        import random
        random.shuffle(items)
        items = items[:SOCKS5_MAX]
        log("SOCKS5", f"按 SOCKS5_MAX 截断为 {len(items)} 个")
    return items

def check_one_socks5(item, session):
    """用同一个 Worker 的 /check?socks5= 接口测代理, 返回统一 result 结构。
    item 可以是 'ip:port' 字符串或带元数据的 dict。"""
    if isinstance(item, dict):
        proxy = item.get("proxy", "")
        meta_cc = item.get("country_code", "")
        meta_city = item.get("city", "")
        meta_org = item.get("org", "")
    else:
        proxy, meta_cc, meta_city, meta_org = item, "", "", ""
    host, _, port_s = proxy.partition(":")
    out = {
        "host": host,
        "port": int(port_s or 0),
        "ip": host,
        "country": "",
        "country_code": "",
        "protocol": "socks5",
        "link": f"socks5://{proxy}",
        "status": "failed",
        "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "exit": None,
        "residential": "unknown",
    }
    try:
        url = WORKER_CHECK_URL.split("?")[0] + "?socks5=" + quote(proxy, safe="")
        r = session.get(url, timeout=CHECK_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (gate-checker)"})
        if r.status_code != 200:
            out["error"] = f"HTTP {r.status_code}"
            out["worker_error"] = True
            return out
        j = r.json()
        ok = bool(j.get("success"))
        out["success"] = ok
        out["status"] = "success" if ok else "failed"
        out["latency_ms"] = j.get("responseTime")
        out["colo"] = j.get("colo")
        out["error"] = None if ok else (j.get("error") or j.get("message") or "check failed")
        exit_info = j.get("exit") or {}
        if exit_info:
            asn = exit_info.get("asn") or {}
            org = asn.get("org") or asn.get("name") or meta_org or ""
            cc = (exit_info.get("country_code") or meta_cc or "").upper()
            out["country"] = exit_info.get("country") or ""
            out["country_code"] = cc
            out["exit"] = {"ip": exit_info.get("ip"), "country": exit_info.get("country"), "country_code": cc, "city": exit_info.get("city") or meta_city, "continent": exit_info.get("continent"), "asn": asn.get("asn"), "org": org, "type": asn.get("type"), "is_datacenter": exit_info.get("is_datacenter")}
            out["residential"] = classify_network(out["host"], org, exit_info.get("is_datacenter"))
        else:
            out["country_code"] = meta_cc
            out["residential"] = classify_network(out["host"], meta_org or None, None)
        return out
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["worker_error"] = True
        return out

def check_all_socks5(proxies, session):
    results = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = [pool.submit(check_one_socks5, p, session) for p in proxies]
        for fut in as_completed(futures):
            results.append(fut.result())
    return results

# ---------------------------------------------------------------------------
# 检测 Worker
# ---------------------------------------------------------------------------
def classify_network(host, exit_org, is_datacenter=None):
    if is_datacenter is True: return "datacenter"
    if is_datacenter is False: return "residential"
    org = (exit_org or "").upper()
    if org:
        if any(k in org for k in DATA_CENTER_ORG_KEYWORDS): return "datacenter"
        if any(k in org for k in RESIDENTIAL_ORG_KEYWORDS): return "residential"
    h = host.lower()
    if h.startswith("public-vpn"): return "datacenter"
    if re.match(r"^vpn\d{5,}", h) or re.match(r"^vpnv\d+", h): return "residential"
    return "unknown"

def check_one(node, session):
    url = WORKER_CHECK_URL + quote(f"{node['host']}:{node['port']}", safe="")
    out = dict(node)
    out["protocol"] = "sstp"
    out["link"] = f"sstp://vpn:vpn@{node['host']}:{node['port']}"
    out["status"] = "failed"
    out["checked_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out["exit"] = None
    out["residential"] = "unknown"
    try:
        r = session.get(url, timeout=CHECK_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (gate-checker)"})
        if r.status_code != 200:
            out["error"] = f"HTTP {r.status_code}"
            out["worker_error"] = True
            return out
        j = r.json()
        ok = bool(j.get("success"))
        out["success"] = ok
        out["status"] = "success" if ok else "failed"
        out["latency_ms"] = j.get("responseTime")
        out["colo"] = j.get("colo")
        out["error"] = (None if ok else (j.get("error") or j.get("message") or "check failed"))
        exit_info = j.get("exit") or {}
        if exit_info:
            asn = exit_info.get("asn") or {}
            org = asn.get("org") or asn.get("name") or ""
            out["exit"] = {"ip": exit_info.get("ip"), "country": exit_info.get("country"), "country_code": exit_info.get("country_code"), "city": exit_info.get("city"), "continent": exit_info.get("continent"), "asn": asn.get("asn"), "org": org, "type": asn.get("type"), "is_datacenter": exit_info.get("is_datacenter")}
            out["residential"] = classify_network(out["host"], org, exit_info.get("is_datacenter"))
        else:
            out["residential"] = classify_network(out["host"], None, None)
        return out
    except Exception as exc:
        out["error"] = f"{type(exc).__name__}: {exc}"
        out["worker_error"] = True
        return out

def check_all(nodes, session):
    results = []
    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = [pool.submit(check_one, n, session) for n in nodes]
        for fut in as_completed(futures):
            results.append(fut.result())
    return results

# ---------------------------------------------------------------------------
# 生成数据
# ---------------------------------------------------------------------------
def build_outputs(results, raw_count, sstp_count, source, socks5_stats=None):
    available = [r for r in results if r.get("success")]
    countries = {}
    for n in available:
        c = n["country"] or "未知"
        countries.setdefault(c, {"code": n["country_code"] or "?", "nodes": []})["nodes"].append(n)

    sstp_ok = sum(1 for n in available if n.get("protocol") == "sstp")
    socks5_ok = sum(1 for n in available if n.get("protocol") == "socks5")
    stats = {"raw_nodes": raw_count, "sstp_nodes": sstp_count, "checked": len(results), "success": len(available), "failed": len(results) - len(available), "countries": len(countries), "residential_est": sum(1 for n in available if n["residential"] == "residential"), "datacenter_est": sum(1 for n in available if n["residential"] == "datacenter"), "sstp_ok": sstp_ok, "socks5_ok": socks5_ok}
    if socks5_stats:
        stats.update(socks5_stats)
    by_country = {}
    for name, grp in countries.items():
        grp["count"] = len(grp["nodes"])
        grp["residential"] = sum(1 for n in grp["nodes"] if n["residential"] == "residential")
        grp["datacenter"] = sum(1 for n in grp["nodes"] if n["residential"] == "datacenter")
        grp["nodes"].sort(key=lambda n: (n.get("latency_ms") is None, n.get("latency_ms") or 0, n["host"]))
        by_country[name] = grp

    data = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"), "source": source, "worker": WORKER_CHECK_URL, "stats": stats, "countries": by_country, "available": available}
    return data

# edgetunnel 入口地址池
# ---- VLESS/Trojan 链式节点补充 (由 vless_nodes.txt 提供, 与 SSTP 同格式) ----
VLESS_NODES_FILE = os.environ.get("VLESS_NODES_FILE", os.path.join(REPO_DIR, "vless_nodes.txt"))

# ---- VLESS 自动挖矿: 公开源 -> __probe 实测 -> 只留真活 ----
VLESS_PROBE_BASE = os.environ.get("VLESS_PROBE_BASE", "https://rentianye25.de5.net/?__probe=")
VLESS_PROBE_ENABLED = os.environ.get("VLESS_PROBE", "on").strip().lower() not in ("off", "0", "false", "no")
VLESS_MINE_ENABLED = os.environ.get("VLESS_MINE", "on").strip().lower() not in ("off", "0", "false", "no")
VLESS_MINE_MAX = int(os.environ.get("VLESS_MINE_MAX", "400"))

VLESS_SOURCES = [
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Configs/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/peasoft/NoMoreWalls/master/list.txt",
    "https://raw.githubusercontent.com/mahdibland/V2RayAggregator/master/Eternity.txt",
    "https://raw.githubusercontent.com/ripaojiedian/freenode/main/sub",
    "https://raw.githubusercontent.com/ermaozi/get_subscribe/main/subscribe/v2ray.txt",
    "https://raw.githubusercontent.com/mheidari98/.proxy/main/all",
    "https://raw.githubusercontent.com/ALIILAPRO/v2rayNG-Config/main/server.txt",
]

ENTRY_POOL = [
    "saas.sin.fan:443", "cdn.204910.best:443", "www.mfyx.cn:443", "p.etime.vip:443",
    "cdn.ctn32.us.kg:443", "cf.877774.xyz:443", "spring.io:443", "cf.nyanya.moe:443",
    "www.sloomb.com:443", "op.chinwa.eu.cc:443", "www.leics.police.uk:443", "securecircle.com:443",
]

def _probe_vless_link(link):
    """实测单条 vless 链接, 返回 (ok, cc, dt)。"""
    try:
        u = VLESS_PROBE_BASE + quote(link, safe="")
        r = requests.get(u, timeout=HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0 (gate-checker)"})
        if r.status_code != 200:
            return False, "", None
        j = r.json()
        if not j.get("connectOK"):
            return False, "", None
        resp = j.get("resp") or ""
        m = re.search(r"([A-Z]{2})", resp)
        return True, (m.group(1) if m else ""), j.get("dt")
    except Exception:
        return False, "", None

def _mine_vless():
    """从公开源抓 vless, 实测后返回存活节点行(入口#备注$vless://...)."""
    if not (VLESS_MINE_ENABLED and VLESS_PROBE_ENABLED):
        return []
    cands, seen = [], set()
    for url in VLESS_SOURCES:
        try:
            r = requests.get(url, timeout=HTTP_TIMEOUT, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                continue
            text = r.text
            items = []
            for ln in text.splitlines():
                ln = ln.strip()
                if ln.startswith("vless://"):
                    items.append(ln)
                elif "://" not in ln and len(ln) > 40:
                    try:
                        dec = base64.b64decode(ln + "=" * (-len(ln) % 4)).decode("utf-8", "replace")
                        for x in dec.splitlines():
                            x = x.strip()
                            if x.startswith("vless://"):
                                items.append(x)
                    except Exception:
                        pass
            for l in items:
                if l not in seen:
                    seen.add(l)
                    cands.append(l)
            log("MINE", "源 %s -> %d 条 (累计 %d)" % (url.split("/")[4] if "/" in url else "?", len(items), len(cands)))
        except Exception as exc:
            log("MINE", "源失败 %s %s" % (url[:50], exc))
        if len(cands) >= VLESS_MINE_MAX * 3:
            break
    cands = cands[:VLESS_MINE_MAX]
    log("MINE", "候选 %d 条, 开始实测" % len(cands))
    alive = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(_probe_vless_link, l): l for l in cands}
        for fut in as_completed(futs):
            ok, cc, dt = fut.result()
            if ok:
                alive.append((futs[fut], cc, dt))
                log("MINE", "复活 [%s] %sms %s" % (cc, dt, futs[fut][:60]))
    alive.sort(key=lambda x: (x[1] or "ZZ", x[2] or 999999))
    log("MINE", "实测 %d -> 真活 %d" % (len(cands), len(alive)))
    # 生成节点行
    lines = []
    per = {}
    for i, (link, cc, dt) in enumerate(alive):
        zh = COUNTRY_ZH.get((cc or "").upper(), (cc or "未知"))
        per[zh] = per.get(zh, 0) + 1
        entry = ENTRY_POOL[i % len(ENTRY_POOL)]
        lines.append("%s#%s-机房-M%02d$%s" % (entry, zh, per[zh], link))
    return lines

def _vless_block():
    """优先用自动挖矿结果(每轮实测), 失败则回退到 vless_nodes.txt。"""
    mined = _mine_vless()
    if mined:
        return mined
    try:
        with open(VLESS_NODES_FILE, "r", encoding="utf-8") as f:
            return [ln.strip() for ln in f if ln.strip() and not ln.strip().startswith("#")]
    except Exception:
        return []

EDGE_HOSTS = [
    h.strip()
    for h in os.environ.get(
        "EDGE_HOSTS",
        "saas.sin.fan:443,cdn.204910.best:443,www.mfyx.cn:443,p.etime.vip:443,cdn.ctn32.us.kg:443,cf.877774.xyz:443,spring.io:443,"
        "cf.nyanya.moe:443,www.sloomb.com:443,op.chinwa.eu.cc:443,www.leics.police.uk:443,securecircle.com:443,www.shopify.com:443,"
        "www.carousell.sg:443,www.dbs.com.sg:443,openai.com:443,linear.app:443,www.bilibili.com:443,uspto.gov:443,www.vmware.com:443",
    ).split(",")
    if h.strip()
]

NODES_URL = os.environ.get("NODES_URL", "https://movelive04.github.io/gate/nodes.txt")

def _chain_suffix(n):
    """节点尾部链式代理指令: SSTP 走 vpn:vpn@host:port, SOCKS5 走 socks5://host:port"""
    if n.get("protocol") == "socks5":
        return f"$socks5://{n['host']}:{n['port']}"
    return f"$sstp://vpn:vpn@{n['host']}:{n['port']}"

def build_nodes_text(data):
    """生成纯节点行版本 (无注释): 每行 = 入口地址#名字$sstp://...
    全局按延迟升序排序: 低延迟住宅节点优先 (跨国家混排)。"""
    countries = data["countries"]
    _entry = os.environ.get("HOSTS_ENTRY", "").strip()
    edge = [e.strip() for e in _entry.split(",") if e.strip()] or EDGE_HOSTS
    lines = []
    idx = 0

    # 收集全部节点(住宅优先), 全局按延迟排序
    pool = []
    for cname, grp in countries.items():
        code = str(grp.get("code") or "?").upper()
        zh = COUNTRY_ZH.get(code) or (code if code and code != "?" else cname)
        for n in grp["nodes"]:
            pool.append((zh, n))
    # 排序键: 住宅优先 -> 延迟升序 -> 主机名
    pool.sort(key=lambda t: (
        0 if t[1].get("residential") == "residential" else 1,
        t[1].get("latency_ms") is None,
        t[1].get("latency_ms") or 999999,
        t[1].get("host") or "",
    ))
    # 按国家内的序号命名
    per_country = {}
    for zh, n in pool:
        if PURE_MODE and n.get("residential") != "residential":
            continue
        per_country[zh] = per_country.get(zh, 0) + 1
        kind = "住宅" if n.get("residential") == "residential" else "机房"
        entry = edge[idx % len(edge)]
        idx += 1
        lines.append(f"{entry}#{zh}-{kind}-{per_country[zh]:02d}{_chain_suffix(n)}")
    # 追加 VLESS 链式节点 (纯净节点补充)
    lines.extend(_vless_block())
    return "\n".join(lines) + "\n"

def write_outputs(data):
    os.makedirs(PUBLIC_DIR, exist_ok=True)
    data_path = os.path.join(PUBLIC_DIR, "data.json")
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)

    html_path = os.path.join(PUBLIC_DIR, "index.html")
    if os.path.exists(TEMPLATE_HTML):
        with open(TEMPLATE_HTML, "r", encoding="utf-8") as f:
            html = f.read()
    else:
        html = ("<html><head><meta charset='utf-8'><title>VPN Gate SSTP 节点</title></head>"
                "<body><h1>VPN Gate SSTP 节点</h1><pre id='out'></pre></body>"
                "<script>fetch('data.json').then(r=>r.json()).then(d=>out.textContent=JSON.stringify(d.stats)).catch(e=>out.textContent='加载失败:'+e)</script></html>")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    nodes_path = os.path.join(PUBLIC_DIR, "nodes.txt")
    with open(nodes_path, "w", encoding="utf-8") as f:
        f.write(build_nodes_text(data))

    return data_path, html_path, nodes_path

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    session = requests.Session()
    rows, source = fetch_vpngate()
    raw_count = len(rows)
    if raw_count == 0:
        die("VPN Gate 返回 0 个原始节点 (数据源异常, 不允许生成空结果)")

    sstp_nodes = to_sstp_nodes(rows)
    sstp_count = len(sstp_nodes)
    if sstp_count == 0:
        die(f"从 {raw_count} 个原始节点中没有解析出任何 SSTP(TCP) 节点 — 数据格式可能已变化, 需要人工适配")
    uniq = dedupe(sstp_nodes)

    if MAX_CHECK_NODES > 0:
        uniq = uniq[:MAX_CHECK_NODES]

    log("VPN GATE", f"获取原始节点: {raw_count}")
    log("VPN GATE", f"SSTP 节点: {sstp_count}")
    log("VPN GATE", f"去重后: {len(uniq)}")

    log("CLOUDFLARE WORKER", f"提交检测: {len(uniq)} (并发 {CONCURRENCY}, 单请求超时 {CHECK_TIMEOUT}s)")
    t0 = time.time()
    results = check_all(uniq, session)
    elapsed = time.time() - t0

    # 纯净模式: 剔除机房 SSTP, 只留住宅
    if PURE_MODE:
        before = len(results)
        results = [r for r in results if (not r.get("success")) or r.get("residential") == "residential"]
        kept_ok = sum(1 for r in results if r.get("success"))
        log("VPN GATE", f"纯净过滤(仅住宅): 成功节点保留 {kept_ok} (原成功 {sum(1 for r in results if r.get('success')) + (before - len(results))})")

    # --- 多源 SOCKS5 代理 (补充出口池) ---
    socks5_stats = None
    if SOCKS5_ENABLED:
        log("SOCKS5", "开始抓取多源 SOCKS5 代理")
        socks5_proxies = fetch_socks5_sources()
        if socks5_proxies:
            log("SOCKS5", f"提交检测: {len(socks5_proxies)} 个代理")
            t1 = time.time()
            socks5_results = check_all_socks5(socks5_proxies, session)
            socks5_elapsed = time.time() - t1
            socks5_ok = [r for r in socks5_results if r.get("success")]
            socks5_errs = [r for r in socks5_results if r.get("worker_error")]
            # 纯净模式: 剔除机房 SOCKS5, 只留住宅
            if SOCKS5_RESIDENTIAL_ONLY:
                before = len(socks5_results)
                socks5_results = [r for r in socks5_results if r.get("residential") == "residential"]
                socks5_ok = [r for r in socks5_results if r.get("success")]
                log("SOCKS5", f"纯净过滤(仅住宅): {before} -> {len(socks5_results)} (成功 {len(socks5_ok)})")
            log("SOCKS5", f"检测成功: {len(socks5_ok)} / {len(socks5_proxies)} (耗时 {socks5_elapsed:.1f}s)")
            results = results + socks5_results
            socks5_stats = {"socks5_raw": len(socks5_proxies), "socks5_ok": len(socks5_ok)}
        else:
            log("SOCKS5", "未获取到任何代理, 跳过")

    success = [r for r in results if r.get("success")]
    failed = [r for r in results if not r.get("success")]
    worker_errors = [r for r in failed if r.get("worker_error")]

    log("CLOUDFLARE WORKER", f"检测成功: {len(success)}")
    log("CLOUDFLARE WORKER", f"检测失败: {len(failed)}" + (f" (其中 Worker 异常 {len(worker_errors)})" if worker_errors else ""))
    log("CLOUDFLARE WORKER", f"耗时: {elapsed:.1f}s")

    if uniq and not success and len(worker_errors) == len(uniq):
        die("Worker 全部请求异常, 检测服务不可用 — 本次运行判定失败 (不生成空结果)")

    data = build_outputs(results, raw_count, sstp_count, source, socks5_stats)
    log("RESULT", f"可用节点: {len(success)}")
    log("RESULT", f"国家数量: {data['stats']['countries']}")

    data_path, html_path, nodes_path = write_outputs(data)
    log("WEBSITE", f"生成 {os.path.relpath(data_path, REPO_DIR)}")
    log("WEBSITE", f"生成 {os.path.relpath(html_path, REPO_DIR)}")
    log("WEBSITE", f"生成 {os.path.relpath(nodes_path, REPO_DIR)}")
    log("USAGE", f"自动轮换: 把 {NODES_URL} 填入 edgetunnel 后台「自定义优选IP」框 (一次配置, 之后每 30 分钟自动更新)")
    log("WEBSITE", "完成 (GitHub Pages 部署由 workflow 执行)")

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        die(f"程序异常: {type(exc).__name__}: {exc}")
