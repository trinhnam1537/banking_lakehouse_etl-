"""Dang nhap OneDrive ca nhan MOT lan, luu refresh token vao Secrets Manager.

Reader `onedrive` (sourcing/readers/onedrive.py) dung refresh token nay de chay
hang ngay khong can ai dang nhap. KHONG nhap, KHONG luu mat khau o dau ca: ban
dang nhap tren trang cua Microsoft trong trinh duyet, script chi nhan token.

Can truoc (docs/sourcing/onedrive.md muc 2):
  - App dang ky tren Azure (Entra ID), loai "Personal Microsoft accounts only",
    bat "Allow public client flows", quyen Graph uy nhiem Files.Read + offline_access.
  - Secret rong `fingraph/src/oauth/onedrive` (repo ha tang tao) va quyen ghi no
    (profile `fingraph` = TerraformAdmin).

Chay:
    python scripts/onedrive_login.py --client-id <Application (client) ID>
    python scripts/onedrive_login.py --client-id <id> --folder "Tai lieu"   # kiem tra doc duoc thu muc
    python scripts/onedrive_login.py --secret fingraph/src/oauth/onedrive   # dang nhap lai, giu client_id cu

Lam lai khi job bao "refresh token het han hoac bi thu hoi" (khong chay > 90 ngay,
doi mat khau tai khoan Microsoft, hoac thu hoi quyen app tai account.live.com/consent).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request

import boto3

LOGIN = "https://login.microsoftonline.com"
GRAPH = "https://graph.microsoft.com/v1.0"
SCOPE = "Files.Read offline_access"


def _norm(name: str) -> str:
    """Giong sourcing/readers/onedrive.py: NFC + khong phan biet hoa thuong."""
    return unicodedata.normalize("NFC", name).casefold()


def _post(url: str, form: dict) -> dict:
    req = urllib.request.Request(url, data=urllib.parse.urlencode(form).encode(),
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode("utf-8", "replace") or "{}")


def device_login(client_id: str, tenant: str) -> dict:
    code = _post(f"{LOGIN}/{tenant}/oauth2/v2.0/devicecode", {"client_id": client_id, "scope": SCOPE})
    if "device_code" not in code:
        sys.exit(f"Khong lay duoc device code: {code.get('error')} {code.get('error_description', '')}\n"
                 f"Kiem tra client id, loai tai khoan cua app va 'Allow public client flows' = Yes.")
    print("\n" + code["message"] + "\n")  # "To sign in, use a web browser to open ... enter the code ..."

    interval = int(code.get("interval", 5))
    deadline = time.time() + int(code.get("expires_in", 900))
    while time.time() < deadline:
        time.sleep(interval)
        tok = _post(f"{LOGIN}/{tenant}/oauth2/v2.0/token", {
            "client_id": client_id,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            "device_code": code["device_code"],
        })
        err = tok.get("error")
        if not err:
            return tok
        if err == "slow_down":
            interval += 5
        elif err != "authorization_pending":
            sys.exit(f"Dang nhap that bai: {err} {tok.get('error_description', '')[:300]}")
    sys.exit("Het thoi gian cho dang nhap (15 phut). Chay lai.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--client-id", help="Application (client) ID cua app Azure. Bo trong = lay tu secret.")
    ap.add_argument("--secret", default="fingraph/src/oauth/onedrive")
    ap.add_argument("--tenant", default="consumers", help="consumers = tai khoan Microsoft ca nhan")
    ap.add_argument("--profile", default="fingraph", help="AWS profile co quyen PutSecretValue")
    ap.add_argument("--region", default="ap-southeast-1")
    ap.add_argument("--folder", help="Thu muc OneDrive de thu liet ke sau khi dang nhap")
    ap.add_argument("--folder-id", help="ID thu muc (resid= trong link chia se) de thu liet ke file")
    args = ap.parse_args()

    # --profile "" -> credential tu bien moi truong (boto3 KHONG dung cache MFA cua AWS CLI:
    # `aws configure export-credentials --profile fingraph --format powershell | Invoke-Expression`)
    sm = boto3.Session(profile_name=args.profile or None, region_name=args.region).client("secretsmanager")
    client_id = args.client_id
    if not client_id:
        try:
            client_id = json.loads(sm.get_secret_value(SecretId=args.secret)["SecretString"])["client_id"]
        except Exception as e:  # noqa: BLE001
            sys.exit(f"Khong co --client-id va khong doc duoc client_id tu secret {args.secret}: {e}")

    tok = device_login(client_id, args.tenant)
    if "refresh_token" not in tok:
        sys.exit("Microsoft khong tra refresh_token — app thieu quyen offline_access.")

    me = json.loads(urllib.request.urlopen(urllib.request.Request(
        f"{GRAPH}/me/drive?$select=driveType,owner",
        headers={"Authorization": f"Bearer {tok['access_token']}"}), timeout=60).read())
    owner = (me.get("owner") or {}).get("user", {}).get("displayName", "?")
    print(f"Dang nhap OK: drive {me.get('driveType')} cua {owner}")

    if args.folder:
        item = json.loads(urllib.request.urlopen(urllib.request.Request(
            f"{GRAPH}/me/drive/root/children?$top=200",
            headers={"Authorization": f"Bearer {tok['access_token']}"}), timeout=60).read())
        first = args.folder.replace("\\", "/").strip("/").split("/")[0]
        names = [c["name"] for c in item.get("value", []) if "folder" in c]
        hit = [n for n in names if _norm(n) == _norm(first)]
        print(f"Thu muc o goc: {names}")
        print(f"  '{first}': {'CO' if hit else 'KHONG THAY'}")

    if args.folder_id:
        fid = urllib.parse.quote(args.folder_id.strip(), safe="!")
        auth = {"Authorization": f"Bearer {tok['access_token']}"}
        try:
            node = json.loads(urllib.request.urlopen(urllib.request.Request(
                f"{GRAPH}/me/drive/items/{fid}?$select=name,folder,parentReference", headers=auth), timeout=60).read())
            kids = json.loads(urllib.request.urlopen(urllib.request.Request(
                f"{GRAPH}/me/drive/items/{fid}/children?$top=50&$select=name,size,file,lastModifiedDateTime",
                headers=auth), timeout=60).read()).get("value", [])
            print(f"Thu muc '{node.get('name')}' ({(node.get('parentReference') or {}).get('path', '')}): "
                  f"{len(kids)} muc")
            for k in kids:
                print(f"  {'F' if 'file' in k else 'D'} {k['name']}  {k.get('size', 0)} B  {k.get('lastModifiedDateTime')}")
        except urllib.error.HTTPError as e:
            print(f"KHONG doc duoc folder_id {args.folder_id}: HTTP {e.code}. Thu muc co nam trong OneDrive "
                  f"CUA tai khoan vua dang nhap khong?")

    sm.put_secret_value(SecretId=args.secret, SecretString=json.dumps({
        "client_id": client_id,
        "tenant": args.tenant,
        "refresh_token": tok["refresh_token"],
        "scope": SCOPE,
        "rotated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    }))
    print(f"Da ghi refresh token vao secret {args.secret}. Khong can lam lai tru khi job bao het han.")


if __name__ == "__main__":
    main()
