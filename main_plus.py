############################################################
# 用户数据迁移工具 — Flask Web UI (SSE实时进度版)
############################################################

import io
import json
import os
import random
import time
import uuid
import zipfile
from datetime import datetime, timedelta

import requests
from flask import (Flask, Response, render_template_string,
                   request, send_file, session, stream_with_context)

app = Flask(__name__)
app.secret_key = os.urandom(24)

BASE_URL          = "https://api.rst-game.com"
ACCOUNTS_DIR      = "data/accounts"
MASTERDATA_PATH   = "../server/game_backend/data/masterdata.json"
EVENT_RANKING_DIR = "data/event_ranking"
# 服务端内存存储，key=player_id，value=token
_token_store: dict[int, str] = {}

BASE_HEADERS = {
    "Host": "api.rst-game.com",
    "User-Agent": "UnityPlayer/2021.3.36f1 (UnityWebRequest/1.0, libcurl/8.5.0-DEV)",
    "Accept": "*/*",
    "os": "android",
    "version": "278",
    "X-Unity-Version": "2021.3.36f1",
    "Content-Type": "application/x-www-form-urlencoded",
}

PROXIES = {
    # "http": "http://127.0.0.1:8888",
    # "https": "http://127.0.0.1:8888",
}

# ── 共用CSS ───────────────────────────────────────────────
COMMON_CSS = """
  *{box-sizing:border-box;margin:0;padding:0}
  body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
       background:#f0f2f5;min-height:100vh;padding:32px 16px;color:#1a1a2e}
  .wrap{max-width:560px;margin:0 auto;display:flex;flex-direction:column;gap:16px}
  .card{background:#fff;border-radius:12px;padding:28px;
        box-shadow:0 2px 16px rgba(0,0,0,.07)}
  h2{font-size:19px;font-weight:700;margin-bottom:20px}
  label{display:block;font-size:12px;color:#666;margin-bottom:5px;margin-top:12px}
  input{width:100%;padding:10px 13px;border:1px solid #ddd;border-radius:8px;
        font-size:14px;outline:none;transition:border .2s}
  input:focus{border-color:#4f6ef7}
  .btn-row{display:flex;gap:10px;margin-top:18px}
  button{flex:1;padding:11px;border:none;border-radius:8px;font-size:14px;
         font-weight:600;cursor:pointer;transition:all .2s}
  button:disabled{opacity:.45;cursor:not-allowed}
  .btn-p{background:#4f6ef7;color:#fff}
  .btn-e{background:#f7924f;color:#fff}
  .btn-dl{background:#10b981;color:#fff;margin-top:10px;width:100%;display:none}
  .btn-p:not(:disabled):hover{background:#3a57d4}
  .btn-e:not(:disabled):hover{background:#d97a3a}
  .btn-dl:not(:disabled):hover{background:#059669}
  .highlight-box{display:none;background:#1a1a2e;border-radius:10px;
                 padding:20px 24px;color:#fff}
  .highlight-box.show{display:block}
  .hl-row{margin-bottom:12px}
  .hl-row:last-child{margin-bottom:0}
  .hl-label{font-size:11px;color:#aab;letter-spacing:.05em;text-transform:uppercase;
            margin-bottom:4px}
  .hl-value{font-size:22px;font-weight:700;letter-spacing:.1em;color:#7ee8a2;
            word-break:break-all}
  .hl-value.uuid-val{font-size:12px;color:#90b8f8;letter-spacing:.03em;font-weight:400}
  .log-box{background:#111827;border-radius:10px;padding:16px;min-height:60px;
           max-height:300px;overflow-y:auto;font-family:monospace;font-size:13px;
           line-height:1.55;color:#d1fae5;display:none}
  .log-box.show{display:block}
  .log-err{color:#fca5a5}
  .log-warn{color:#fde68a}
  .log-ok{color:#6ee7b7}
  .notice{background:#fffbeb;border:1px solid #fde68a;border-radius:10px;
          padding:18px 20px}
  .notice h3{font-size:13px;font-weight:700;color:#92400e;margin-bottom:10px}
  .notice p{font-size:12px;color:#78350f;line-height:1.65;margin-bottom:6px}
  .notice p:last-child{margin-bottom:0}
  .notice .red{color:#dc2626;font-weight:600}
  .lang-link{text-align:right;font-size:12px;color:#999}
  .lang-link a{color:#4f6ef7;text-decoration:none}
  .lang-link a:hover{text-decoration:underline}
"""

NOTICE_ZH = """
    <p>1. 本网站为《Re:ステージ！(リステップ)》用户数据保存工具，在 7月31日12:00 服务器关闭之前均可正常使用。本工具仅用于学习与技术研究目的，不用于任何商业用途。使用过程中如出现数据异常、账号异常或其他任何问题，开发者概不负责。</p>
    <p class="red">⚠️ 使用第三方工具可能违反游戏服务条款，存在账号限制或封禁风险，请自行承担后果。</p>
    <p class="red">⚠️ 点击左边按钮以后，会显示新的继承码，请务必马上截图保存！请勿随意刷新界面！</p>
    <p class="red">⚠️ 本网站仅供 Android 手机用户使用！iOS（苹果）用户使用可能导致有偿钻石数据异常或丢失，请谨慎使用！</p>
    <p>左侧按钮用于保存除"历史活动排名"之外的全部用户数据，执行速度较快；右侧按钮用于保存全部历史活动排名数据，由于数据量较大，执行时间可能较长，请耐心等待。</p>
    <p>4. 本项目代码已完全开源，GitHub 地址：xxx。欢迎学习与二次开发，但请遵守开源协议。</p>
"""

NOTICE_JA = """
    <p>1. 本サイトは《Re:ステージ！(リステップ)》のユーザーデータ保存ツールです。7月31日12:00のサービス終了まで利用可能です。本ツールは学習・技術研究目的のみに使用され、商業目的には使用しません。データ異常・アカウント異常等の問題が発生した場合、開発者は一切責任を負いません。</p>
    <p class="red">⚠️ サードパーティツールの使用はゲームの利用規約に違反する可能性があり、アカウント制限・停止のリスクがあります。自己責任でご利用ください。</p>
    <p class="red">⚠️ 左ボタンを押すと新しい引継ぎコードが表示されます。必ずすぐにスクリーンショットを撮って保存してください！画面を更新しないでください！</p>
    <p class="red">⚠️ 本サイトはAndroidユーザー専用です！iOS（Apple）ユーザーが使用すると、有償ジュエルのデータ異常・消失が発生する可能性があります。ご注意ください！</p>
    <p>左ボタンは「過去イベントランキング」を除く全ユーザーデータを保存します（30秒程度で終わる）。右ボタンは全過去イベントランキングを保存します（データ量が多いため時間がかかる場合があります）。</p>
    <p>4. 本プロジェクトのコードは完全にオープンソースです。GitHub：xxx。学習・二次開発歓迎ですが、オープンソースライセンスを遵守してください。</p>
"""

# ── HTML テンプレート ─────────────────────────────────────
def make_html(lang="zh"):
    lang_link = '<a href="/ja">🇯🇵 日本語版はこちら</a>' if lang == "zh" else '<a href="/">🇨🇳 中国語版はこちら</a>'
    title     = "用户数据迁移工具" if lang == "zh" else "ユーザーデータ移行ツール"
    lbl_id    = "用户 ID（player_id）" if lang == "zh" else "ユーザーID（player_id）"
    lbl_code  = "继承码" if lang == "zh" else "引継ぎコード"
    btn_mig   = "迁移数据" if lang == "zh" else "データ移行"
    btn_evt   = "抓取活动排名" if lang == "zh" else "イベントランキング取得"
    btn_dl1   = "⬇ 下载迁移数据" if lang == "zh" else "⬇ 移行データをダウンロード"
    btn_dl2   = "⬇ 下载活动排名数据" if lang == "zh" else "⬇ イベントランキングをダウンロード"
    hl_code   = "新引继码" if lang == "zh" else "新しい引継ぎコード"
    hl_uuid   = "UUID（请妥善保存）" if lang == "zh" else "UUID（必ず保存してください）"
    notice_h  = "⚠️ 注意事项" if lang == "zh" else "⚠️ 注意事項"
    notice_c  = NOTICE_ZH if lang == "zh" else NOTICE_JA
    alert1    = "请填写用户ID和继承码" if lang == "zh" else "ユーザーIDと引継ぎコードを入力してください"
    alert2    = "请先完成左边的迁移数据步骤" if lang == "zh" else "先に左側のデータ移行を完了してください"
    err_conn  = "✗ 连接中断" if lang == "zh" else "✗ 接続が切断されました"

    return f"""<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>{COMMON_CSS}</style>
</head>
<body>
<div class="wrap">
  <div class="lang-link">{lang_link}</div>

  <div class="card">
    <h2>🎮 {title}</h2>
    <label>{lbl_id}</label>
    <input id="player_id" placeholder="2049590">
    <label>{lbl_code}</label>
    <input id="handover_code" placeholder="XXXXXXXXX">
    <div class="btn-row">
      <button class="btn-p" id="btn-migrate" onclick="startTask('migrate')">{btn_mig}</button>
      <button class="btn-e" id="btn-event"   onclick="startTask('event')" disabled>{btn_evt}</button>
    </div>
    <button class="btn-dl" id="btn-dl-migrate" onclick="downloadData('migrate')">{btn_dl1}</button>
    <button class="btn-dl" id="btn-dl-event"   onclick="downloadData('event')">{btn_dl2}</button>
  </div>

  <div class="highlight-box" id="highlight-box">
    <div class="hl-row">
      <div class="hl-label">{hl_code}</div>
      <div class="hl-value" id="hl-code">—</div>
    </div>
    <div class="hl-row">
      <div class="hl-label">{hl_uuid}</div>
      <div class="hl-value uuid-val" id="hl-uuid">—</div>
    </div>
  </div>

  <div class="log-box" id="log-box"></div>

  <div class="notice">
    <h3>{notice_h}</h3>
    {notice_c}
  </div>
</div>
<script>
let migratePlayer = null;

function startTask(action) {{
  const pid  = document.getElementById('player_id').value.trim();
  const code = document.getElementById('handover_code').value.trim();
  if (!pid || !code) {{ alert('{alert1}'); return; }}
  if (action === 'event' && !migratePlayer) {{ alert('{alert2}'); return; }}

  document.getElementById('btn-migrate').disabled = true;
  document.getElementById('btn-event').disabled   = true;

  const logBox = document.getElementById('log-box');
  logBox.innerHTML = '';
  logBox.classList.add('show');

  let reqUrl;
  if (action === 'migrate') {{
    const myUuid = (crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2)).replace(/-/g, '');
    document.getElementById('hl-uuid').textContent = myUuid;
    document.getElementById('highlight-box').classList.add('show');
    reqUrl = `/run?action=migrate&player_id=${{pid}}&handover_code=${{encodeURIComponent(code)}}&my_uuid=${{myUuid}}`;
  }} else {{
    reqUrl = `/run?action=event&player_id=${{migratePlayer}}`;
  }}

  const es = new EventSource(reqUrl);
  es.onmessage = function(e) {{
    const d = JSON.parse(e.data);
    if (d.type === 'log') {{
      const line = document.createElement('div');
      const t = d.text;
      if (t.startsWith('✓'))      line.className = 'log-ok';
      else if (t.startsWith('✗')) line.className = 'log-err';
      else if (t.startsWith('⚠')) line.className = 'log-warn';
      line.textContent = t;
      logBox.appendChild(line);
      logBox.scrollTop = logBox.scrollHeight;
    }}
    if (d.type === 'code') {{
      document.getElementById('hl-code').textContent = d.text;
    }}
    if (d.type === 'migrate_done') {{
      migratePlayer = d.player_id;
      document.getElementById('btn-event').disabled = false;
      document.getElementById('btn-dl-migrate').style.display = 'block';
      es.close();
    }}
    if (d.type === 'event_done') {{
      document.getElementById('btn-dl-event').style.display = 'block';
      es.close();
    }}
    if (d.type === 'done') {{
      es.close();
    }}
  }};
  es.onerror = function() {{
    const line = document.createElement('div');
    line.className = 'log-err';
    line.textContent = '{err_conn}';
    logBox.appendChild(line);
    es.close();
  }};
}}

function downloadData(type) {{
  const pid = migratePlayer || document.getElementById('player_id').value.trim();
  window.location.href = `/download?type=${{type}}&player_id=${{pid}}`;
}}
</script>
</body>
</html>"""

# ── 工具函数 ──────────────────────────────────────────────

def load_masterdata():
    with open(MASTERDATA_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_account_file(account_id, filename, data):
    folder = os.path.join(ACCOUNTS_DIR, f"data_{account_id}")
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, filename)
    with open(path, "w", encoding="utf-8") as f:
        if isinstance(data, str):
            f.write(data)
        else:
            json.dump(data, f, ensure_ascii=False, indent=2)


def rpost(session_obj, token, path, data=None):
    """带随机延迟的POST请求"""
    time.sleep(random.uniform(1.0, 2.0))
    h = BASE_HEADERS.copy()
    h["TOKEN"] = token
    resp = session_obj.post(
        f"{BASE_URL}{path}", headers=h,
        data=data or {}, proxies=PROXIES, verify=False, timeout=15
    )
    return resp.json()


def sse(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"

def _check_backup_limit(player_id: int) -> str | None:
    """返回错误信息字符串，None表示允许继续"""
    folder = os.path.join(ACCOUNTS_DIR, f"data_{player_id}")
    uuid_path = os.path.join(folder, "uuid")
    if not os.path.exists(uuid_path):
        return None  # 没有历史数据，允许

    mtime = datetime.fromtimestamp(os.path.getmtime(uuid_path))
    now   = datetime.now()
    delta = now - mtime

    # 最后一周（7.24-7.31）放宽到1小时
    in_last_week = now >= datetime(2025, 7, 24, 0, 0, 0)
    if in_last_week:
        if delta.total_seconds() < 3600:
            last_str = mtime.strftime("%Y-%m-%d %H:%M")
            return f"✗ 该用户距离上次备份不足1小时（上次：{last_str}），请稍后再试"
    else:
        if delta.days < 7:
            last_str = mtime.strftime("%Y-%m-%d %H:%M")
            return f"✗ 该用户距离上次备份不超过7天（上次：{last_str}），请7天后再备份"
    return None

def build_achieve_progress(achieve_list_data, r1, r2, masterdata):
    d1 = r1.get("data", {})
    d2 = r2.get("data", {})
    characters  = d2.get("characters", [])
    char_lv_map = {c["character_id"]: c["lv"] for c in characters}

    remote_progress = {item["id"]: item["progress"] for item in achieve_list_data}

    aid_max = {}
    for a in masterdata.get("achievement", []):
        if a["achievement_kind"] != 1:
            continue
        aid = a["achievement_id"]
        if a["id"] in remote_progress:
            p = remote_progress[a["id"]]
            if aid not in aid_max or p > aid_max[aid]:
                aid_max[aid] = p

    normal = {
        "live_count":    aid_max.get(1,  0),
        "live_fc_count": aid_max.get(2,  0),
        "total_score":   aid_max.get(3,  0),
        "login_count":   aid_max.get(4,  0),
        "card_count":    aid_max.get(24, 0),
        "acce_count":    aid_max.get(25, 0),
        "ipp":           aid_max.get(26, 0),
    }

    normal_map_vals = {
        1: normal["live_count"], 2: normal["live_fc_count"],
        3: normal["total_score"], 4: normal["login_count"],
        5: d1.get("lv", 1), 24: normal["card_count"],
        25: normal["acce_count"], 26: normal["ipp"],
    }

    claimed, watermark = [], {}
    for a in masterdata.get("achievement", []):
        if a["achievement_kind"] != 1:
            continue
        aid, cond_v = a["achievement_id"], a["condition_value"]
        if a.get("condition_kind") == 10:
            progress = char_lv_map.get(a.get("condition_op", 0), 1)
        else:
            progress = normal_map_vals.get(aid, aid_max.get(aid, 0))
        if progress >= cond_v:
            claimed.append(a["id"])
            wk = str(aid)
            if cond_v > watermark.get(wk, 0):
                watermark[wk] = cond_v

    today = datetime.now()
    if today.hour < 4:
        today -= timedelta(days=1)
    today_str = today.strftime("%Y-%m-%d")
    monday    = (today.date() - timedelta(days=today.date().weekday())).strftime("%Y-%m-%d")

    return {
        "claimed": claimed, "daily_achievement_id": 1,
        "weekly_achievement_id": 1, "last_daily_update": today_str,
        "last_weekly_update": monday, "daily": {}, "weekly": {},
        "normal": normal, "limited": {},
        "normal_claimed_watermark": watermark, "ck_12": 0,
    }


# ── 迁移流程 ──────────────────────────────────────────────

def run_migrate(player_id, handover_code, my_uuid):
    sess = requests.Session()

    yield sse({"type": "log", "text": f"UUID: {my_uuid}"})

    try:
        masterdata = load_masterdata()
    except Exception as e:
        yield sse({"type": "log", "text": f"✗ 读取masterdata失败: {e}"})
        yield sse({"type": "done"})
        return

    err = _check_backup_limit(player_id)
    if err:
        yield sse({"type": "log", "text": err})
        yield sse({"type": "done"})
        return

    # handover
    try:
        time.sleep(random.uniform(1.0, 2.0))
        resp = sess.post(
            f"{BASE_URL}/account/handover", headers=BASE_HEADERS,
            data={"handover_code": handover_code,
                  "handover_player_id": player_id, "uuid": my_uuid},
            proxies=PROXIES, verify=False, timeout=15
        )
        r = resp.json()
    except Exception as e:
        yield sse({"type": "log", "text": f"✗ handover 异常: {e}"})
        yield sse({"type": "done"})
        return

    if r.get("code") != 200:
        yield sse({"type": "log", "text": "✗ 继承失败：继承码或用户名错误"})
        yield sse({"type": "done"})
        return
    token = r.get("token")
    yield sse({"type": "log", "text": "✓ handover 成功"})

    # ── 第一轮 login ──────────────────────────────────────
    h = BASE_HEADERS.copy(); h["UUID"] = my_uuid
    try:
        time.sleep(random.uniform(1.0, 2.0))
        r1a = sess.post(f"{BASE_URL}/account/login1st", headers=h,
                        data={}, proxies=PROXIES, verify=False, timeout=15).json()
    except Exception as e:
        yield sse({"type": "log", "text": f"✗ login1st 异常: {e}"})
        yield sse({"type": "done"}); return
    if r1a.get("code") != 200:
        yield sse({"type": "log", "text": f"✗ login1st 失败，请保存UUID: {my_uuid}"})
        yield sse({"type": "done"}); return
    token = r1a.get("token")
    yield sse({"type": "log", "text": "✓ login1st 成功（第一轮）"})

    r3a = rpost(sess, token, "/account/login3rd", {"token": token})
    if r3a.get("code") != 200:
        yield sse({"type": "log", "text": f"✗ login3rd 失败，请保存UUID: {my_uuid}"})
        yield sse({"type": "done"}); return
    token = r3a.get("token") or token
    yield sse({"type": "log", "text": "✓ login3rd 成功（第一轮）"})

    r2a = rpost(sess, token, "/account/login2nd", {"token": token})
    if r2a.get("code") != 200:
        yield sse({"type": "log", "text": f"✗ login2nd 失败，请保存UUID: {my_uuid}"})
        yield sse({"type": "done"}); return
    token = r2a.get("token") or token
    yield sse({"type": "log", "text": "✓ login2nd 成功（第一轮）"})

    # handover_code
    hc = rpost(sess, token, "/account/handover_code")
    if hc.get("code") != 200:
        yield sse({"type": "log", "text": f"✗ handover_code 失败，请保存UUID: {my_uuid}"})
        yield sse({"type": "done"}); return
    token = hc.get("token") or token
    new_code = hc.get("data", {}).get("handover_code", "")
    yield sse({"type": "log",  "text": f"✓ 新引继码: {new_code}"})
    yield sse({"type": "code", "text": new_code})

    # ── 第二轮 login ──────────────────────────────────────
    h2 = BASE_HEADERS.copy(); h2["UUID"] = my_uuid
    try:
        time.sleep(random.uniform(1.0, 2.0))
        r1 = sess.post(f"{BASE_URL}/account/login1st", headers=h2,
                       data={}, proxies=PROXIES, verify=False, timeout=15).json()
    except Exception as e:
        yield sse({"type": "log", "text": f"✗ login1st 异常（第二轮）: {e}"})
        yield sse({"type": "done"}); return
    if r1.get("code") != 200:
        yield sse({"type": "log", "text": f"✗ login1st 失败（第二轮），请保存UUID: {my_uuid}"})
        yield sse({"type": "done"}); return
    token = r1.get("token")
    yield sse({"type": "log", "text": "✓ login1st 成功（第二轮）"})

    r3 = rpost(sess, token, "/account/login3rd", {"token": token})
    if r3.get("code") != 200:
        yield sse({"type": "log", "text": f"✗ login3rd 失败（第二轮），请保存UUID: {my_uuid}"})
        yield sse({"type": "done"}); return
    token = r3.get("token") or token
    yield sse({"type": "log", "text": "✓ login3rd 成功（第二轮）"})

    r2 = rpost(sess, token, "/account/login2nd", {"token": token})
    if r2.get("code") != 200:
        yield sse({"type": "log", "text": f"✗ login2nd 失败（第二轮），请保存UUID: {my_uuid}"})
        yield sse({"type": "done"}); return
    token = r2.get("token") or token
    yield sse({"type": "log", "text": "✓ login2nd 成功（第二轮）"})

    # 保存 login 文件
    for name, rd in [("login1st", r1), ("login2nd", r2), ("login3rd", r3)]:
        save_account_file(player_id, name, {k: v for k, v in rd.items() if k != "token"})
    save_account_file(player_id, "uuid", my_uuid)
    save_account_file(player_id, "handover_code", {k: v for k, v in hc.items() if k != "token"})
    yield sse({"type": "log", "text": "✓ login文件 / UUID / handover_code 已保存"})

    # ranking/private
    rank_data = rpost(sess, token, "/ranking/private", {"friend_id": player_id})
    token = rank_data.get("token") or token
    if rank_data.get("code") == 200:
        save_account_file(player_id, "ranking_private",
                          {k: v for k, v in rank_data.items() if k != "token"})
        yield sse({"type": "log", "text": "✓ ranking/private 已保存"})
    else:
        yield sse({"type": "log", "text": "⚠ ranking/private 跳过"})

    # titlelist
    title_data = rpost(sess, token, "/profile/titlelist")
    token  = title_data.get("token") or token
    titles = []
    if title_data.get("code") == 200:
        titles = [t["title_id"] for t in title_data.get("data", [])]
        yield sse({"type": "log", "text": f"✓ titlelist 成功，{len(titles)} 个称号"})
    else:
        yield sse({"type": "log", "text": "⚠ titlelist 跳过"})

    # item/list
    items_raw, page = [], 1
    while True:
        item_data = rpost(sess, token, "/item/list", {"page_no": page})
        token = item_data.get("token") or token
        if item_data.get("code") != 200:
            break
        batch = item_data.get("data", [])
        items_raw.extend(batch)
        if not batch or item_data.get("page", 0) == 0:
            break
        page += 1
    yield sse({"type": "log", "text": f"✓ item/list 成功，{len(items_raw)} 件道具"})

    # achieve/list
    ach_data = rpost(sess, token, "/achieve/list")
    token = ach_data.get("token") or token
    achieve_list_items = ach_data.get("data", []) if ach_data.get("code") == 200 else []
    yield sse({"type": "log", "text": f"✓ achieve/list 成功，{len(achieve_list_items)} 条"})

    # follow / follower
    follow_ids, follower_ids = [], []
    fd = rpost(sess, token, "/friend/followlist", {"page_no": 1})
    token = fd.get("token") or token
    if fd.get("code") == 200:
        follow_ids = [u["user_id"] for u in fd.get("data", [])]
        yield sse({"type": "log", "text": f"✓ followlist {len(follow_ids)} 人"})

    frd = rpost(sess, token, "/friend/followerlist", {"page_no": 1})
    token = frd.get("token") or token
    if frd.get("code") == 200:
        follower_ids = [u["user_id"] for u in frd.get("data", [])]
        yield sse({"type": "log", "text": f"✓ followerlist {len(follower_ids)} 人"})

    # 构建文件
    achieve_progress = build_achieve_progress(achieve_list_items, r1, r2, masterdata)
    save_account_file(player_id, "achieve_progress", achieve_progress)
    yield sse({"type": "log", "text": "✓ achieve_progress 已保存"})

    ipp_val = achieve_progress["normal"].get("ipp", 0)
    d1      = r1.get("data", {})
    login_stats = {
        "total_days": d1.get("total_login_days", 0), "ipp": ipp_val,
        "week_days": 0, "total_friend_pt": d1.get("total_friend_pt", 0),
        "friend_pt": 0, "items": items_raw, "titles": titles,
        "shop_history": {}, "exchange_history": {}, "unit_power": ipp_val,
    }
    save_account_file(player_id, "login_stats", login_stats)
    save_account_file(player_id, "follow_list",   {"ids": follow_ids})
    save_account_file(player_id, "follower_list", {"ids": follower_ids})
    yield sse({"type": "log", "text": "✓ login_stats / follow_list / follower_list 已保存"})

    # 把 token 存到服务端 session，供活动排名继续使用
    _token_store[player_id] = token

    yield sse({"type": "log",         "text": "━━━ 迁移完成 ━━━"})
    yield sse({"type": "migrate_done","player_id": player_id})
    _token_store[player_id] = token

# ── 活动排名流程 ──────────────────────────────────────────

def run_event(player_id):
    token = _token_store.get(player_id)
    os.makedirs(EVENT_RANKING_DIR, exist_ok=True)

    token = _token_store.get(player_id)
    if not token:
        yield sse({"type": "log", "text": "✗ 未找到token，请先完成迁移步骤"})
        yield sse({"type": "done"}); return

    yield sse({"type": "log", "text": "✓ 使用已有token继续请求"})

    sess = requests.Session()

    # ranking/past
    past = rpost(sess, token, "/ranking/past")
    token = past.get("token") or token
    _token_store[player_id] = token
    event_ids = past.get("data", {}).get("event_ids", [])
    yield sse({"type": "log", "text": f"✓ 获取到 {len(event_ids)} 个活动 ID"})

    player_event_stats = {}
    for eid in event_ids:
        r = rpost(sess, token, "/ranking/event", {"event_id": eid})
        token = r.get("token") or token
        _token_store[player_id] = token
        if r.get("code") != 200:
            yield sse({"type": "log", "text": f"⚠ event {eid} 失败"})
            continue

        data   = r.get("data", {})
        top    = data.get("top", [])
        player = data.get("player", [])

        with open(os.path.join(EVENT_RANKING_DIR, f"event_{eid}_top.json"),
                  "w", encoding="utf-8") as f:
            json.dump(top, f, ensure_ascii=False, indent=2)

        if player:
            player_event_stats[str(eid)] = player

        yield sse({"type": "log",
                   "text": f"✓ event {eid}：top {len(top)} 条，player {len(player)} 条"})

    if player_event_stats:
        save_account_file(player_id, "event_ranking_self", player_event_stats)

    yield sse({"type": "log",       "text": "━━━ 活动排名抓取完成 ━━━"})
    yield sse({"type": "event_done"})


# ── Flask 路由 ────────────────────────────────────────────

@app.route("/")
def index_zh():
    return make_html("zh")

@app.route("/ja")
def index_ja():
    return make_html("ja")

@app.route("/run")
def run_sse():
    action        = request.args.get("action", "migrate")
    player_id     = int(request.args.get("player_id", 0))
    handover_code = request.args.get("handover_code", "")
    my_uuid       = request.args.get("my_uuid", uuid.uuid4().hex)

    gen = run_migrate(player_id, handover_code, my_uuid) if action == "migrate" \
          else run_event(player_id)

    return Response(
        stream_with_context(gen),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )

@app.route("/download")
def download():
    dl_type   = request.args.get("type", "migrate")
    player_id = request.args.get("player_id", "")

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        if dl_type == "migrate":
            folder = os.path.join(ACCOUNTS_DIR, f"data_{player_id}")
            if os.path.isdir(folder):
                for fname in os.listdir(folder):
                    fpath = os.path.join(folder, fname)
                    if os.path.isfile(fpath):
                        zf.write(fpath, fname)
        else:
            if os.path.isdir(EVENT_RANKING_DIR):
                for fname in os.listdir(EVENT_RANKING_DIR):
                    fpath = os.path.join(EVENT_RANKING_DIR, fname)
                    if os.path.isfile(fpath):
                        zf.write(fpath, fname)
            self_path = os.path.join(ACCOUNTS_DIR, f"data_{player_id}", "event_ranking_self")
            if os.path.isfile(self_path):
                zf.write(self_path, "event_ranking_self.json")

    buf.seek(0)
    filename = f"migrate_{player_id}.zip" if dl_type == "migrate" else f"event_ranking_{player_id}.zip"
    return send_file(buf, mimetype="application/zip",
                     as_attachment=True, download_name=filename)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000, threaded=True)