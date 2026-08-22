"""MetaMask-based API key onboarding — the wallet key NEVER leaves the wallet.

    python bot.py fleet setup-key-web

Flow:
  1. Generates an Ed25519 API key pair locally.
  2. Serves a one-page site on http://127.0.0.1:8901 and opens your browser.
  3. The page asks MetaMask to sign the CreateApiKey typed data
     (eth_signTypedData_v4) — you just click Connect and Sign.
  4. The signature comes back here; we register the key with the venue,
     verify it is live, and write API_PRIVATE_KEY / API_PUBLIC_KEY /
     L1_ADDRESS (the address that actually signed) into .env.

Your Ethereum private key is never typed, never pasted, never stored.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import webbrowser
from pathlib import Path

from aiohttp import web

from core.config import BASE_URL, CHAIN_ID
from core.rest import ArcusRest
from core.signing import generate_ed25519

PORT = 8901

PAGE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><title>Arcus API key — sign with MetaMask</title>
<style>
body{font-family:Inter,system-ui,sans-serif;background:#0b0e14;color:#e6e9f0;
display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}
.card{background:#12161f;border:1px solid #1f2532;border-radius:16px;padding:36px;max-width:520px}
h1{font-size:19px;margin:0 0 10px}p{color:#7c8494;line-height:1.6;font-size:14px}
button{font:inherit;font-weight:600;background:#8b5cf6;color:#fff;border:0;border-radius:10px;
padding:12px 22px;cursor:pointer;margin-top:16px;font-size:15px}
button:disabled{opacity:.4;cursor:default}
.ok{color:#3ecf8e}.err{color:#f2545b}#status{margin-top:18px;font-size:14px;white-space:pre-wrap}
code{background:#171c27;padding:2px 7px;border-radius:6px;font-size:12px}
</style></head><body><div class="card">
<h1>Arcus API key registration</h1>
<p>This signs the <code>CreateApiKey</code> message with MetaMask.
Your wallet key stays in your wallet — you only approve a signature.
Make sure MetaMask is on the account that holds your Arcus USDG.</p>
<button id="go">Connect MetaMask &amp; Sign</button>
<div id="status"></div>
<script>
const TYPED = __TYPED_DATA__;
const el = (m, cls) => { const s=document.getElementById('status'); s.className=cls||''; s.textContent=m; };
document.getElementById('go').onclick = async () => {
  try {
    if (!window.ethereum) { el('Δεν βρέθηκε MetaMask σε αυτόν τον browser.', 'err'); return; }
    const [addr] = await window.ethereum.request({method:'eth_requestAccounts'});
    el('Υπογραφή στο MetaMask…');
    const sig = await window.ethereum.request({
      method:'eth_signTypedData_v4', params:[addr, JSON.stringify(TYPED)]
    });
    el('Καταχώρηση στο Arcus…');
    const r = await fetch('/submit', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({address: addr, signature: sig})});
    const d = await r.json();
    if (d.ok) { el('✅ ' + d.message + '\\nΜπορείς να κλείσεις αυτή τη σελίδα.', 'ok');
      document.getElementById('go').disabled = true; }
    else el('✗ ' + d.error, 'err');
  } catch (e) { el('✗ ' + (e.message || e), 'err'); }
};
</script></div></body></html>"""


def _write_env(updates: dict[str, str]) -> Path:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    lines = env_path.read_text().splitlines() if env_path.exists() else []
    seen: set[str] = set()
    for i, line in enumerate(lines):
        key = line.split("=", 1)[0].strip()
        if key in updates:
            lines[i] = f"{key}={updates[key]}"
            seen.add(key)
    for key, value in updates.items():
        if key not in seen:
            lines.append(f"{key}={value}")
    env_path.write_text("\n".join(lines) + "\n")
    return env_path


async def run() -> int:
    account_index = int(os.environ.get("ACCOUNT_INDEX", "0") or "0")
    priv_hex, pub_hex = generate_ed25519()
    valid_until = int(time.time() * 1000) + 170 * 86_400_000

    # EXACTLY the typed data the venue verifies (docs: Arcus API Key / v1 /
    # rootchain chainId; index-less type allowed for accountIndex 0).
    if account_index == 0:
        cak_fields = [
            {"name": "apiWalletName", "type": "string"},
            {"name": "apiWalletPublicKey", "type": "string"},
            {"name": "validUntil", "type": "uint256"},
        ]
        message = {
            "apiWalletName": "rh-arcus-fleet",
            "apiWalletPublicKey": pub_hex,
            "validUntil": valid_until,
        }
    else:
        cak_fields = [
            {"name": "apiWalletName", "type": "string"},
            {"name": "apiWalletPublicKey", "type": "string"},
            {"name": "validUntil", "type": "uint256"},
            {"name": "accountIndex", "type": "uint8"},
        ]
        message = {
            "apiWalletName": "rh-arcus-fleet",
            "apiWalletPublicKey": pub_hex,
            "validUntil": valid_until,
            "accountIndex": account_index,
        }
    typed = {
        "domain": {"name": "Arcus API Key", "version": "1", "chainId": CHAIN_ID},
        "types": {
            "EIP712Domain": [
                {"name": "name", "type": "string"},
                {"name": "version", "type": "string"},
                {"name": "chainId", "type": "uint256"},
            ],
            "CreateApiKey": cak_fields,
        },
        "primaryType": "CreateApiKey",
        "message": message,
    }

    done: asyncio.Future[int] = asyncio.get_event_loop().create_future()

    async def index(_r: web.Request) -> web.Response:
        return web.Response(
            text=PAGE.replace("__TYPED_DATA__", json.dumps(typed)),
            content_type="text/html",
        )

    async def submit(request: web.Request) -> web.Response:
        body = await request.json()
        address = body["address"]
        sig = body["signature"]
        if sig.startswith("0x"):
            sig = sig[2:]
        r, s, v = "0x" + sig[0:64], "0x" + sig[64:128], "0x" + sig[128:130]
        rest = ArcusRest(BASE_URL)
        await rest.start()
        try:
            await rest.create_api_key(
                {
                    "address": address,
                    "publicKey": pub_hex,
                    "apiWalletName": "rh-arcus-fleet",
                    "validUntil": valid_until,
                    "accountIndex": account_index,
                    "signature": {"r": r, "s": s, "v": v},
                }
            )
            live = False
            for _ in range(30):
                keys = await rest.api_keys(address)
                if any(
                    (k.get("apiKey") or k.get("publicKey") or "").lower() == pub_hex.lower()
                    for k in keys
                ):
                    live = True
                    break
                await asyncio.sleep(1)
        except Exception as exc:
            return web.json_response({"ok": False, "error": str(exc)})
        finally:
            await rest.close()
        if not live:
            return web.json_response(
                {"ok": False, "error": "key accepted but not visible on /v1/apiKeys yet — retry"}
            )
        env_path = _write_env(
            {
                "L1_ADDRESS": address,
                "ACCOUNT_INDEX": str(account_index),
                "API_PRIVATE_KEY": priv_hex,
                "API_PUBLIC_KEY": pub_hex,
            }
        )
        print(f"\nAPI key registered & VERIFIED for {address}.")
        print(f"Credentials written to {env_path}.")
        print("Next: python3 bot.py fleet prove && python3 bot.py fleet start")
        if not done.done():
            done.set_result(0)
        return web.json_response(
            {"ok": True, "message": f"API key registered for {address}. .env updated."}
        )

    app = web.Application()
    app.router.add_get("/", index)
    app.router.add_post("/submit", submit)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "127.0.0.1", PORT).start()
    url = f"http://127.0.0.1:{PORT}"
    print(f"Άνοιξε (ή άνοιξα ήδη) το {url} — Connect MetaMask & Sign.")
    print("Το wallet key σου δεν φεύγει ποτέ από το MetaMask. Ctrl-C για ακύρωση.")
    webbrowser.open(url)
    try:
        return await done
    finally:
        await runner.cleanup()
