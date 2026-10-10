"""
Kiểm tra từng nguồn dữ liệu có dùng được từ vị trí đang chạy hay không.

Chạy ở hai nơi và so sánh kết quả:

  # Tren may ban (IP Viet Nam)
  python producers/check_sources.py

  # Tren EC2 Singapore — noi producer thuc su chay
  aws ssm send-command --instance-ids <id> --document-name AWS-RunShellScript \
      --parameters file://params.json --profile fingraph

Vì sao phải test ở cả hai nơi: Binance và một số nguồn chặn theo vùng địa lý.
Việt Nam và AWS ap-southeast-1 là hai vùng khác nhau — dùng được ở đây không
có nghĩa là dùng được ở kia. Đây là việc của tuần 1 trong bảng 8.1, và kết
quả của nó quyết định producer dùng Binance hay Finnhub.

Chỉ dùng thư viện chuẩn để chạy được ngay trên EC2 mà không cần pip install.
"""

from __future__ import annotations

import json
import socket
import ssl
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field

TIMEOUT = 15

# SEC EDGAR TỪ CHỐI request không có User-Agent mô tả danh tính.
# Quy định của họ: "Sample User-Agent: Sample Company Name AdminContact@example.com"
# Thiếu header này là 403, và thông báo lỗi không nói rõ lý do.
SEC_UA = "HUST FinGraph Research trinhnama31609@gmail.com"


@dataclass
class Result:
    name: str
    ok: bool
    detail: str
    ms: int = 0
    sample: str = ""


@dataclass
class Check:
    name: str
    url: str
    headers: dict = field(default_factory=dict)
    note: str = ""


REST_CHECKS = [
    Check("binance-ping", "https://api.binance.com/api/v3/ping",
          note="Endpoint re nhat de biet co bi chan vung khong"),
    Check("binance-trades", "https://api.binance.com/api/v3/trades?symbol=BTCUSDT&limit=2",
          note="Du lieu that"),
    Check("binance-vision", "https://data-api.binance.vision/api/v3/ping",
          note="Endpoint du lieu cong khai, it bi chan hon api.binance.com"),
    Check("gleif", "https://api.gleif.org/api/v1/lei-records?page%5Bsize%5D=1",
          note="Danh tinh phap nhan cho tang ontology"),
    Check("sec-edgar", "https://data.sec.gov/submissions/CIK0000320193.json",
          headers={"User-Agent": SEC_UA},
          note="BAT BUOC co User-Agent mo ta danh tinh"),
    Check("fred", "https://fred.stlouisfed.org/",
          note="Chi kiem tra reachable; API that can api_key"),
]

WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@trade"


def check_rest(c: Check) -> Result:
    req = urllib.request.Request(c.url, headers=c.headers or {})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
            body = r.read(400).decode("utf-8", "replace")
            ms = int((time.time() - t0) * 1000)
            return Result(c.name, True, f"HTTP {r.status}", ms, body[:120])
    except urllib.error.HTTPError as e:
        ms = int((time.time() - t0) * 1000)
        # 451 = Unavailable For Legal Reasons -> dau hieu dien hinh cua chan vung
        hint = "  <<< CHAN VUNG" if e.code in (403, 451) else ""
        return Result(c.name, False, f"HTTP {e.code}{hint}", ms)
    except Exception as e:  # noqa: BLE001
        ms = int((time.time() - t0) * 1000)
        return Result(c.name, False, f"{type(e).__name__}: {e}", ms)


def check_websocket() -> Result:
    """Bắt tay WebSocket thủ công, không cần thư viện ngoài.

    Chỉ cần biết kết nối có lên được và có bản tin chảy về hay không —
    không cần giải mã frame đầy đủ.
    """
    import base64
    import os
    import re

    host, port, path = "stream.binance.com", 9443, "/ws/btcusdt@trade"
    key = base64.b64encode(os.urandom(16)).decode()
    t0 = time.time()

    try:
        raw = socket.create_connection((host, port), timeout=TIMEOUT)
        ctx = ssl.create_default_context()
        sock = ctx.wrap_socket(raw, server_hostname=host)

        handshake = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        )
        sock.sendall(handshake.encode())

        resp = sock.recv(1024).decode("utf-8", "replace")
        if "101" not in resp.split("\r\n")[0]:
            sock.close()
            return Result("binance-websocket", False,
                          f"bat tay that bai: {resp.splitlines()[0] if resp else 'khong phan hoi'}")

        # Doc mot frame; payload la JSON nen chi can moc phan {...}
        sock.settimeout(TIMEOUT)
        data = sock.recv(8192)
        sock.close()
        ms = int((time.time() - t0) * 1000)

        m = re.search(rb"\{.*\}", data, re.S)
        if not m:
            return Result("binance-websocket", True, "ket noi OK nhung chua co ban tin", ms)
        return Result("binance-websocket", True, "ket noi OK, co ban tin", ms,
                      m.group(0).decode("utf-8", "replace")[:120])

    except Exception as e:  # noqa: BLE001
        ms = int((time.time() - t0) * 1000)
        return Result("binance-websocket", False, f"{type(e).__name__}: {e}", ms)


def public_ip() -> str:
    for url in ("https://checkip.amazonaws.com", "https://ifconfig.me/ip"):
        try:
            with urllib.request.urlopen(url, timeout=8) as r:
                return r.read().decode().strip()
        except Exception:  # noqa: BLE001
            continue
    return "khong xac dinh"


def main() -> int:
    ip = public_ip()
    print("=" * 74)
    print(f"KIEM TRA NGUON DU LIEU   IP: {ip}   {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 74)

    results = [check_rest(c) for c in REST_CHECKS]
    results.append(check_websocket())

    for r in results:
        mark = "OK  " if r.ok else "FAIL"
        print(f"[{mark}] {r.name:<20} {r.ms:>6} ms  {r.detail}")
        if r.sample:
            print(f"        {r.sample}")

    failed = [r.name for r in results if not r.ok]
    print("-" * 74)
    if failed:
        print(f"THAT BAI: {', '.join(failed)}")
        print("Neu binance-* that bai o day nhung OK o may khac -> bi chan vung.")
        print("Phuong an thay the: Finnhub, hoac chi dung bo phat lai Kaggle.")
    else:
        print("Tat ca nguon deu dung duoc tu vi tri nay.")

    # In JSON de tien luu ket qua vao _ops/benchmark/ lam bang chung
    print("-" * 74)
    print(json.dumps({
        "ip": ip,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "results": [{"name": r.name, "ok": r.ok, "detail": r.detail, "ms": r.ms}
                    for r in results],
    }, indent=2))

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
