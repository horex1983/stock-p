"""
app_public.py — 급등주 모멘텀 대시보드 (공개 배포용)
데이터 소스: P1 → GitHub JSON → _github_json() 단일 경로
"""

import os, json, base64, logging, requests, re
from collections import Counter
import streamlit as st
import streamlit.components.v1 as _components
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="급등주 모멘텀 대시보드", page_icon="📈",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
.metric-card { background: #f8f9fa; border: 1px solid #dee2e6;
    border-radius: 8px; padding: 12px 16px; margin-bottom: 8px; }
.metric-label { color: #6c757d; font-size: 0.78em; margin-bottom: 2px; }
.metric-value { color: #212529; font-size: 1.1em; font-weight: 700; }
.up   { color: #d32f2f !important; }
.down { color: #1565c0 !important; }
.section-title { color: #212529; font-size: 1.05em; font-weight: 700;
    border-left: 3px solid #2e7d32; padding-left: 8px; margin: 16px 0 8px 0; }
section[data-testid="stSidebar"] { width: 340px !important; min-width: 340px !important; }
section[data-testid="stSidebar"] > div:first-child { width: 340px !important; }
</style>""", unsafe_allow_html=True)


def _secret(key, default=""):
    try:
        if key in st.secrets: return st.secrets[key]
    except: pass
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


GITHUB_TOKEN        = _secret("GITHUB_TOKEN")
GITHUB_REPO         = _secret("GITHUB_REPO")
GITHUB_BRANCH       = _secret("GITHUB_BRANCH", "main")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


import pathlib as _pathlib
_CACHE_DIR = _pathlib.Path("/tmp/p2_cache")
_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _period_contains_today(period_str: str) -> bool:
    """'YYYY.MM.DD ~ YYYY.MM.DD' 형식에서 오늘 포함 여부 (P1 app_utils 동일 로직)"""
    if not period_str:
        return False
    m = re.findall(r'(\d{4})[.\-](\d{2})[.\-](\d{2})', period_str)
    if len(m) < 2:
        return False
    _today_iso = datetime.now().date().isoformat()
    s = f"{m[0][0]}-{m[0][1]}-{m[0][2]}"
    e = f"{m[1][0]}-{m[1][1]}-{m[1][2]}"
    return s <= _today_iso <= e


@st.cache_data(ttl=300)
def _github_json(path):
    _file = _CACHE_DIR / path.replace("/", "__")
    if GITHUB_TOKEN and GITHUB_REPO:
        try:
            r = requests.get(
                f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}",
                headers={"Authorization": f"Bearer {GITHUB_TOKEN}",
                         "Accept": "application/vnd.github+json"},
                params={"ref": GITHUB_BRANCH}, timeout=10)
            if r.status_code == 200:
                meta = r.json()
                raw_content = meta.get("content")
                if raw_content:
                    # normal path: content base64-encoded (<1 MB)
                    data = json.loads(base64.b64decode(raw_content).decode())
                else:
                    # file >1 MB: GitHub omits content, use download_url (raw)
                    dl_url = meta.get("download_url")
                    if not dl_url:
                        raise ValueError("no content and no download_url")
                    r2 = requests.get(dl_url, timeout=30)
                    r2.raise_for_status()
                    data = r2.json()
                _file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
                return data
        except: pass
    # fallback: last saved local cache
    if _file.exists():
        try:
            return json.loads(_file.read_text(encoding="utf-8"))
        except: pass
    return None


def get_theme_news():
    d = _github_json("data/accumulated.json")
    return d.get("items", []) if isinstance(d, dict) else []

def get_surge_reasons():
    d = _github_json("data/surge_reasons_db.json")
    return d if isinstance(d, dict) else {}

def get_rsi_snapshot():
    d = _github_json("data/rsi_snapshot.json")
    return d if isinstance(d, dict) else {}

def get_cb_overhang():
    d = _github_json("data/cb_overhang_cache.json")
    return d if isinstance(d, dict) else {}

def get_meta():
    d = _github_json("data/_meta.json")
    return d if isinstance(d, dict) else {}

def get_watchlist():
    d = _github_json("data/watchlist.json")
    if isinstance(d, list): return d
    if isinstance(d, dict): return d.get("items", d.get("tickers", []))
    return []

def get_surge_table():
    """P1 exported surge table (tier/theme/risk already computed)."""
    d = _github_json("data/surge_table.json")
    return d if isinstance(d, list) else []

@st.cache_data(ttl=300)
def get_surge_history():
    """최근 5거래일 급등주 스냅샷 슬라이딩 윈도우."""
    d = _github_json("data/surge_history.json")
    if isinstance(d, dict):
        return d.get("snapshots", [])
    return []

def get_indices_p1():
    """P1 exported index data: KOSPI/KOSDAQ/NASDAQ/S&P500 + disparity."""
    d = _github_json("data/indices.json")
    return d if isinstance(d, dict) else {}

@st.cache_data(ttl=300)
def get_indices_history():
    """최근 30거래일 지수 시계열 슬라이딩 윈도우."""
    d = _github_json("data/indices_history.json")
    if isinstance(d, dict):
        return d.get("series", [])
    return []

def get_krx_listing():
    """P1 exported KRX full listing for stock search."""
    d = _github_json("data/krx_listing.json")
    return d if isinstance(d, list) else []

def get_accumulated_news():
    """Return (surge_items, theme_items) split by category from accumulated.json."""
    d = _github_json("data/accumulated.json")
    items = d.get("items", []) if isinstance(d, dict) else []
    surge = [it for it in items if it.get("category", "") == "특징 상한가 및 급등종목"]
    theme = [it for it in items if it.get("category", "") == "특징 테마"]
    return surge, theme


# ── 특징주/테마 파싱 (P1 infostock_crawler.py 동일 로직) ──────────────────────

_ACTION = r'(?:상한가|급등|급락|강세|약세|상승|하락|반등)'

def _parse_body_entries(text: str) -> list:
    """Parse 특징주 body text → [{name, code, rate, reason}, ...]"""
    parts = re.split(r'\n?\s*\(([0-9A-Z]{6})\)', text)
    results = []
    n = (len(parts) - 1) // 2
    for i in range(n):
        code = parts[2 * i + 1]
        body = (parts[2 * i + 2] if 2 * i + 2 < len(parts) else "").strip()
        prev = parts[2 * i]
        if i == 0:
            name = ""
            for line in reversed([l.strip() for l in prev.split('\n') if l.strip()]):
                if re.search(r'\d+[원%]|\([+\-]', line):
                    continue
                clean = re.sub(r'^.*?사유', '', line).strip() or line
                if re.search(r'[가-힣]', clean):
                    name = clean; break
        else:
            m = re.search(_ACTION + r'([A-Za-z가-힣\u4e00-\u9fff·&]{1,10})\s*$', prev)
            if m:
                name = m.group(1)
            else:
                name = ""
                for line in reversed([l.strip() for l in prev.split('\n') if l.strip()]):
                    if re.search(r'\d+[원%]|\([+\-]', line):
                        continue
                    if re.search(r'[가-힣]', line):
                        candidate = re.sub(_ACTION + r'.*', '', line).strip()
                        if len(candidate) <= 12:
                            name = candidate
                        break
        rate_match = re.search(r'\([+\-][\d.]+%\)', body)
        rate_str = rate_match.group(0) if rate_match else ''
        if rate_match:
            after = body[rate_match.end():].strip()
            after = re.sub(r'^(\d)(\d{4}[년])', r'\2', after)
            after = re.sub(r'^(\d)(?=[가-힣])', '', after).strip()
            after = re.sub(r'^\d\n', '', after).strip()
            after = re.sub(
                _ACTION + r'([A-Za-z가-힣\u4e00-\u9fff·&(]+)$',
                lambda m: m.group(0)[:m.group(0).index(m.group(1))], after.strip())
            after = re.sub(
                r'(상한가|급등|급락|강세|약세|상승|하락|반등)\n+[가-힣A-Za-z\u4e00-\u9fff·&]+\s*$',
                r'\1', after)
            reason = after.strip()
        else:
            reason = body.strip()
        results.append({'name': name, 'code': code, 'rate': rate_str, 'reason': reason})
    return results

def _parse_theme_entries(text: str) -> list:
    """Extract ▷ bullet entries from 특징테마 body text."""
    idx = text.find('테마시황')
    if idx == -1:
        return [l.strip() for l in text.split('\n') if l.strip().startswith('▷')]
    sub = text[idx:]
    results = []
    for line in sub.split('\n'):
        line = line.strip()
        if line.startswith('▷'):
            results.append(line)
        elif results and line and not line.startswith('▷'):
            break
    return results


# ── 키워드 추출 (P1 infostock_keyword.py 동일 로직) ──────────────────────────

_KW_STOPWORDS = {
    "상한가","상한","하한가","하한","급등","급락","강세","약세","상승","하락","반등","등락",
    "모멘텀","기대감","부각","전망","배경","소식","이슈","이유","지속","추진","관련","테마",
    "공시","발표","속에","가운데","통해","대비","이날","오늘","최근","현재","올해","지난",
    "이번","이후","이전","해당","당일","종목","주가","주식","관련주","업체","기업","회사",
    "사업","영업","분기","연간","수익","실적","억원","조원","가격","규모","시장","거래",
    "투자","매수","매도","증가","감소","확대","축소","개선","수혜","강화","급등주","특징주",
}

def _kw_clean(word: str) -> str:
    word = word.split('/')[0].strip()
    word = re.sub(r'등[가-힣]?$', '', word).strip()
    return word.replace(' ', '')

def _extract_kw(text: str) -> list:
    """'X 테마' 패턴에서 키워드 추출 (P1 _extract_theme_keywords 동일)."""
    m = re.search(r'(.{2,120}?)\s*테마', text)
    if not m:
        return []
    segment = re.sub(r'\([^)]*\)', '', m.group(1)).strip()
    parts = segment.split('/')
    def _pick(part):
        for tok in reversed(part.split()):
            kw = _kw_clean(tok.strip())
            if (len(kw) >= 2 and re.search(r'[가-힣]', kw)
                    and kw not in _KW_STOPWORDS
                    and not re.fullmatch(r'[\d,.\-/]+', kw)
                    and not re.search(r'\d*년[가-힣]?$|\d+[월일]', kw)):
                return kw
        return None
    results = []
    if len(parts) == 1:
        kw = _pick(parts[0])
        if kw: results.append(kw)
    else:
        for p in parts:
            kw = _pick(p)
            if kw and kw not in results: results.append(kw)
    return results

def _get_top_keywords(surge_items: list, n: int = 8) -> list:
    """surge_items에서 테마 키워드 Top-N 추출 → [(word, score), ...]"""
    counter: dict = {}
    for item in surge_items:
        for entry in _parse_body_entries(item.get("text", "")):
            reason = entry.get("reason", "").strip()
            if not reason:
                continue
            try:
                mv = re.search(r'[\d.]+', entry.get("rate", ""))
                rate_val = float(mv.group()) if mv else 0.0
            except Exception:
                rate_val = 0.0
            score = 1.0 + min(rate_val / 30.0, 1.0) * 0.5
            for kw in _extract_kw(reason):
                counter[kw] = counter.get(kw, 0.0) + score
    keys = list(counter.keys())
    for k in keys:
        if k in counter and any(k != o and k in o for o in keys):
            del counter[k]
    return sorted(((w, round(s, 2)) for w, s in counter.items()),
                  key=lambda x: x[1], reverse=True)[:n]


# ── 휴장일 정적 데이터 ──────────────────────────────────────────────────────────

_DOMESTIC_HOLIDAYS = [
    {"date": "1/1(목)",        "reason": "신정"},
    {"date": "2/16(월)~18(수)", "reason": "설 연휴"},
    {"date": "3/2(월)",         "reason": "삼일절(대체)"},
    {"date": "5/1(금)",         "reason": "근로자의 날"},
    {"date": "5/5(화)",         "reason": "어린이날"},
    {"date": "5/25(월)",        "reason": "부처님오신날(대체)"},
    {"date": "6/3(수)",         "reason": "지방선거"},
    {"date": "8/17(월)",        "reason": "광복절(대체)"},
    {"date": "9/24(목)~25(금)", "reason": "추석 연휴"},
    {"date": "10/5(월)",        "reason": "개천절(대체)"},
    {"date": "10/9(금)",        "reason": "한글날"},
    {"date": "12/25(금)",       "reason": "성탄절"},
    {"date": "12/31(목)",       "reason": "연말 휴장"},
]
_US_HOLIDAYS = [
    {"date": "1/1(목)",  "reason": "새해 첫날"},
    {"date": "1/19(월)", "reason": "MK 탄생일"},
    {"date": "2/16(월)", "reason": "대통령의 날"},
    {"date": "4/3(금)",  "reason": "성금요일"},
    {"date": "5/25(월)", "reason": "메모리얼 데이"},
    {"date": "6/19(금)", "reason": "노예해방기념일"},
    {"date": "7/3(금)",  "reason": "독립기념일 대체휴장"},
    {"date": "9/7(월)",  "reason": "노동절"},
    {"date": "11/26(목)","reason": "추수감사절"},
    {"date": "12/25(금)","reason": "크리스마스"},
]


def _github_put(path, content_str):
    """Write/update a file in the GitHub repo. Returns True on success."""
    if not GITHUB_TOKEN or not GITHUB_REPO: return False
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}"
    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}",
               "Accept": "application/vnd.github+json"}
    try:
        r = requests.get(url, headers=headers,
                         params={"ref": GITHUB_BRANCH}, timeout=10)
        sha = r.json().get("sha", "") if r.status_code == 200 else ""
        payload = {"message": f"update {path}",
                   "content": base64.b64encode(content_str.encode()).decode(),
                   "branch": GITHUB_BRANCH}
        if sha:
            payload["sha"] = sha
        r2 = requests.put(url, headers=headers, json=payload, timeout=10)
        return r2.status_code in (200, 201)
    except:
        return False


# ── P1 GitHub 데이터 리더 (공공데이터 API 대체) ──────────────────────────────

@st.cache_data(ttl=300)
def get_market_breadth():
    """시장폭 (상승/하락/보합) — P1 market_breadth.json"""
    d = _github_json("data/market_breadth.json")
    return d if isinstance(d, dict) else {}


@st.cache_data(ttl=300)
def get_watchlist_prices(codes):
    """관심종목 시세 — P1 watchlist_prices.json → DataFrame"""
    d = _github_json("data/watchlist_prices.json")
    if not isinstance(d, dict) or not codes:
        return pd.DataFrame()
    codes_set = {str(c).zfill(6) for c in codes}
    rows = [v for k, v in d.items() if str(k).zfill(6) in codes_set]
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    for col in ["fltRt", "trqu", "clpr"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df.reset_index(drop=True)


@st.cache_data(ttl=3600)
def get_corp_info(ticker):
    """종목별 기업정보+재무+OHLC — P1 corp_info.json"""
    d = _github_json("data/corp_info.json")
    if not isinstance(d, dict):
        return {}
    return d.get(str(ticker).zfill(6), {})


@st.cache_data(ttl=300)
def get_ohlcv(ticker):
    """OHLCV — P1 ohlcv_cache.json (급등주+관심종목 6개월 일봉)."""
    d = _github_json("data/ohlcv_cache.json")
    if not isinstance(d, dict):
        return pd.DataFrame()
    rows = d.get(str(ticker).zfill(6))
    if not rows or not isinstance(rows, list):
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    df.columns = [c.capitalize() for c in df.columns]  # open→Open, etc.
    return df


def render_chart(ticker, name):
    df = get_ohlcv(ticker)
    if df.empty:
        st.caption("차트 데이터 없음")
        return
    df["MA5"]  = df["Close"].rolling(5).mean()
    df["MA20"] = df["Close"].rolling(20).mean()
    df["MA60"] = df["Close"].rolling(60).mean()
    df["BB_std"]   = df["Close"].rolling(20).std()
    df["BB_upper"] = df["MA20"] + 2 * df["BB_std"]
    df["BB_lower"] = df["MA20"] - 2 * df["BB_std"]

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.75, 0.25], vertical_spacing=0.02)
    # Bollinger band fill
    fig.add_trace(go.Scatter(
        x=pd.concat([df.index.to_series(), df.index.to_series()[::-1]]),
        y=pd.concat([df["BB_upper"], df["BB_lower"][::-1]]),
        fill="toself", fillcolor="rgba(99,110,250,0.08)",
        line=dict(color="rgba(0,0,0,0)"), name="BB Band",
        showlegend=False, hoverinfo="skip"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["BB_upper"], name="BB상단",
                             line=dict(color="rgba(99,110,250,0.5)", width=1, dash="dot"),
                             showlegend=False), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["BB_lower"], name="BB하단",
                             line=dict(color="rgba(99,110,250,0.5)", width=1, dash="dot"),
                             showlegend=False), row=1, col=1)
    # Candlestick
    fig.add_trace(go.Candlestick(
        x=df.index, open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"],
        name="가격", increasing_line_color="#c62828", decreasing_line_color="#1565c0",
        increasing_fillcolor="#c62828", decreasing_fillcolor="#1565c0"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["MA5"],  name="MA5",
                             line=dict(color="#2e7d32", width=1.2, dash="dot")), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["MA20"], name="MA20",
                             line=dict(color="#f57c00", width=1.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["MA60"], name="MA60",
                             line=dict(color="#7b1fa2", width=1.5)), row=1, col=1)
    # Volume
    close_v = df["Close"].values.flatten()
    open_v  = df["Open"].values.flatten()
    colors  = ["#c62828" if c >= o else "#1565c0" for c, o in zip(close_v, open_v)]
    vol_ma  = pd.Series(df["Volume"].values.flatten()).rolling(20).mean()
    fig.add_trace(go.Bar(x=df.index, y=df["Volume"].values.flatten(),
                         name="거래량", marker_color=colors, opacity=0.5), row=2, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=vol_ma,
                             name="거래량MA20", line=dict(color="#f57c00", width=1)),
                  row=2, col=1)
    fig.update_layout(
        template="plotly_white", height=550, margin=dict(l=10, r=10, t=30, b=10),
        title=dict(text=f"{name} ({ticker}) - 6개월", font=dict(size=13), x=0),
        xaxis_rangeslider_visible=False, legend=dict(orientation="h", y=1.08, x=0))
    fig.update_yaxes(gridcolor="#eeeeee")
    fig.update_xaxes(gridcolor="#eeeeee")
    st.plotly_chart(fig, use_container_width=True)


@st.cache_data(ttl=300)
def get_dart(ticker):
    """DART 공시 목록 — P1 dart_cache.json (최근 60일)."""
    d = _github_json("data/dart_cache.json")
    if not isinstance(d, dict):
        return []
    items = d.get(str(ticker).zfill(6), [])
    return items if isinstance(items, list) else []


def _tier(flt_rt):
    """Return tier badge based on daily change rate."""
    try:
        v = float(flt_rt)
    except (TypeError, ValueError):
        return "D"
    if v >= 20: return "🏆 S"
    if v >= 15: return "🟢 A"
    if v >= 10: return "🟡 B"
    if v >=  7: return "🟠 C"
    return "🔴 D"


def _make_display_df(df, surge_reasons=None):
    cols = ["순위", "srtnCd", "itmsNm", "mrktCtg", "clpr", "fltRt", "trqu", "basDt"]
    cols = [c for c in cols if c in df.columns]
    disp = df[cols].copy()
    if "fltRt" in disp.columns:
        disp.insert(disp.columns.get_loc("fltRt") + 1, "티어",
                    disp["fltRt"].apply(_tier))
    if surge_reasons is not None:
        disp["급등이유"] = disp["srtnCd"].apply(
            lambda t: surge_reasons.get(str(t).zfill(6), {}).get("reason", "")
            if isinstance(surge_reasons.get(str(t).zfill(6), {}), dict) else "")
    disp = disp.rename(columns={
        "srtnCd": "종목코드", "itmsNm": "종목명", "mrktCtg": "시장",
        "clpr": "종가", "fltRt": "등락률(%)", "trqu": "거래량", "basDt": "기준일"})
    return disp


def render_sidebar(indices_p1, surge_items, theme_items, indices_history=None):
    def _idx_card(label, close_val, chg, status_txt, border_color):
        close_str = f"{close_val:,.2f}" if close_val else "-"
        if chg is None:
            chg_color, arrow, chg_str = "#aaa", "", "-"
        else:
            chg_color = "#c62828" if chg >= 0 else "#1565c0"
            arrow = "▲" if chg >= 0 else "▼"
            chg_str = f"{arrow}{abs(chg):.2f}%"
        status_row = (f"<div style='font-size:0.8em;color:#777;margin-top:2px;'>{status_txt}</div>"
                      if status_txt else "")
        return (
            f"<div style='border-left:3px solid {border_color};background:#FAFBFD;"
            f"border-radius:0 8px 8px 0;padding:8px 10px;margin-bottom:6px;'>"
            f"<div style='font-size:0.75em;font-weight:700;color:#888;letter-spacing:0.5px;'>{label}</div>"
            f"<div style='font-size:1.25em;font-weight:700;color:#1a1a1a;line-height:1.3;'>{close_str}</div>"
            f"<div style='font-size:1.1em;font-weight:700;color:{chg_color};'>{chg_str}</div>"
            f"{status_row}</div>"
        )

    with st.sidebar:
        # ── 디지털 시계 ──────────────────────────────────────────────────────
        _components.html(
            """
            <div style='padding:10px 10px 8px;background:#1E2D4E;border-radius:8px;text-align:center;'>
              <div id='clk' style='font-size:2.6em;font-weight:700;color:#E8ECF4;
                   letter-spacing:4px;font-family:monospace;line-height:1.2;'>--:--:--</div>
              <div id='dat' style='font-size:0.85em;color:#8FA0C0;margin-top:4px;'>----.--.-- (-)</div>
            </div>
            <script>
            var DAYS=['일','월','화','수','목','금','토'];
            function pad(n){return String(n).padStart(2,'0');}
            function tick(){
                var n=new Date();
                document.getElementById('clk').textContent=pad(n.getHours())+':'+pad(n.getMinutes())+':'+pad(n.getSeconds());
                document.getElementById('dat').textContent=n.getFullYear()+'.'+pad(n.getMonth()+1)+'.'+pad(n.getDate())+' ('+DAYS[n.getDay()]+')';
            }
            tick(); setInterval(tick,1000);
            </script>
            """,
            height=110,
        )

        # ── 시장 지수 ────────────────────────────────────────────────────────
        st.markdown(
            "<div style='margin:8px 0 6px;padding-bottom:5px;border-bottom:1px solid #E8ECF4;'>"
            "<strong>🧭 시장지수 현황</strong></div>",
            unsafe_allow_html=True)

        if indices_p1:
            _r1c1, _r1c2 = st.columns(2)
            _idx = {k: v for k, v in indices_p1.items() if isinstance(v, dict)}

            def _get(name):
                it = _idx.get(name, {})
                return (float(it.get("close", 0) or 0),
                        float(it.get("change_pct", 0) or 0),
                        it.get("disparity_status", ""))

            ks_c, ks_chg, ks_st = _get("KOSPI")
            kq_c, kq_chg, kq_st = _get("KOSDAQ")
            nd_c, nd_chg, _     = _get("NASDAQ")
            sp_c, sp_chg, _     = _get("S&P500")

            _r1c1.markdown(_idx_card("KOSPI",  ks_c, ks_chg, ks_st, "#1565C0"), unsafe_allow_html=True)
            _r1c2.markdown(_idx_card("KOSDAQ", kq_c, kq_chg, kq_st, "#E65100"), unsafe_allow_html=True)
            _r2c1, _r2c2 = st.columns(2)
            _r2c1.markdown(_idx_card("NASDAQ", nd_c, nd_chg, "", "#2E7D32"), unsafe_allow_html=True)
            _r2c2.markdown(_idx_card("S&P500", sp_c, sp_chg, "", "#2E7D32"), unsafe_allow_html=True)

            # ── 지수 미니차트 (최근 30거래일) ───────────────────────────────
            if indices_history and len(indices_history) >= 2:
                try:
                    _hist_df = pd.DataFrame(reversed(indices_history))  # 오래된 것부터
                    _hist_df["date"] = pd.to_datetime(_hist_df["date"], format="%Y%m%d")
                    _hist_df = _hist_df.set_index("date")
                    _cols_avail = [c for c in ["KOSPI", "KOSDAQ"] if c in _hist_df.columns]
                    if _cols_avail:
                        with st.expander("📈 지수 추이 (최근 30일)", expanded=False):
                            st.line_chart(
                                _hist_df[_cols_avail].dropna(),
                                height=140,
                                use_container_width=True,
                            )
                except Exception:
                    pass
        else:
            pass

        # ── K200 야간선물 (전체 너비) ─────────────────────────────────────────
        _nf = indices_p1.get("night_futures", {}) if isinstance(indices_p1, dict) else {}
        if _nf and _nf.get("price") is not None:
            _sign   = _nf.get("sign", "3")
            _arrow  = "▲" if _sign in ("1", "2") else ("▼" if _sign in ("4", "5") else "")
            _pct    = _nf.get("change_pct", 0)
            _pct_color = "#c62828" if _sign in ("1", "2") else ("#1565c0" if _sign in ("4", "5") else "#555")
            _api_name  = _nf.get("name", "")
            if _nf.get("is_ls_futures"):
                _label = f"{_api_name} 🌙" if _api_name else "KRX 야간선물 🌙"
            elif _nf.get("is_night"):
                _label = f"{_api_name} 🌙" if _api_name else "K200 야간선물 🌙"
            elif _nf.get("is_index_fallback"):
                _label = "KOSPI200 지수 (야간선물 거래없음)"
            else:
                _label = "K200 선물(전일)"
            st.markdown(
                f"<div style='border-left:3px solid #F9A825;background:#FAFBFD;"
                f"border-radius:0 8px 8px 0;padding:8px 10px;margin-bottom:6px;'>"
                f"<div style='font-size:0.75em;font-weight:700;color:#888;letter-spacing:0.5px;'>{_label}</div>"
                f"<div style='display:flex;align-items:baseline;gap:10px;'>"
                f"<span style='font-size:1.25em;font-weight:700;color:#1a1a1a;'>{_nf['price']:.2f}pt</span>"
                f"<span style='font-size:1.1em;font-weight:700;color:{_pct_color};'>{_arrow}{abs(_pct):.2f}%</span>"
                f"</div></div>",
                unsafe_allow_html=True)

        # ── 시장 폭 ──────────────────────────────────────────────────────────
        breadth = get_market_breadth()
        if breadth:
            up   = breadth.get("상승", 0)
            dn   = breadth.get("하락", 0)
            flat = breadth.get("보합", 0)
            st.markdown(
                f"<div class='metric-card'>"
                f"<div class='metric-label'>시장 폭 (전체 종목)</div>"
                f"<div class='metric-value' style='font-size:0.95em'>"
                f"<span class='up'>▲ {up}</span> &nbsp;"
                f"<span class='down'>▼ {dn}</span> &nbsp;"
                f"<span style='color:#8b949e'>― {flat}</span>"
                f"</div></div>",
                unsafe_allow_html=True)

        # ── 특징주 / 특징테마 팝오버 ─────────────────────────────────────────
        if surge_items or theme_items:
            st.markdown("### 🏷️ 테마 &amp; 뉴스", unsafe_allow_html=True)
            def _fmt_date(item):
                raw = item.get("sendDate", "")
                return f"{raw[:4]}.{raw[4:6]}.{raw[6:]}" if len(raw) == 8 else raw

            col_s, col_t = st.columns(2)

            # 특징주 팝오버
            with col_s:
                if "surge_day_idx" not in st.session_state:
                    st.session_state.surge_day_idx = 0
                with st.popover("특징주", use_container_width=True):
                    if surge_items:
                        c1, c2, c3 = st.columns([1, 4, 1])
                        cur = st.session_state.surge_day_idx
                        if c1.button("◀", key="sp_prev",
                                     disabled=cur >= len(surge_items) - 1):
                            st.session_state.surge_day_idx = cur + 1
                        if c3.button("▶", key="sp_next", disabled=cur == 0):
                            st.session_state.surge_day_idx = cur - 1
                        idx = min(st.session_state.surge_day_idx, len(surge_items) - 1)
                        c2.markdown(f"<div style='text-align:center'>{_fmt_date(surge_items[idx])}</div>",
                                    unsafe_allow_html=True)
                        entries = _parse_body_entries(surge_items[idx].get("text", ""))
                        shown = 0
                        for e in entries:
                            if re.search(r'스팩|SPAC', e.get('name', ''), re.IGNORECASE): continue
                            if re.search(r'신규\s*상장|상장\s*첫날|상장일', e.get('reason', '')): continue
                            try:
                                rv = float(re.search(r'[+\-]?([\d.]+)%', e['rate']).group(1))
                            except: rv = 0.0
                            if rv < 15.0: continue
                            rate_html = f'<span style="color:#f85149">{e["rate"]}</span>'
                            st.markdown(f"**{e['name']}** {rate_html} : {e['reason']}",
                                        unsafe_allow_html=True)
                            shown += 1
                        pass
                    else:
                        pass

            # 특징테마 팝오버
            with col_t:
                if "theme_day_idx" not in st.session_state:
                    st.session_state.theme_day_idx = 0
                with st.popover("특징테마", use_container_width=True):
                    if theme_items:
                        c1, c2, c3 = st.columns([1, 4, 1])
                        cur = st.session_state.theme_day_idx
                        if c1.button("◀", key="tp_prev",
                                     disabled=cur >= len(theme_items) - 1):
                            st.session_state.theme_day_idx = cur + 1
                        if c3.button("▶", key="tp_next", disabled=cur == 0):
                            st.session_state.theme_day_idx = cur - 1
                        idx = min(st.session_state.theme_day_idx, len(theme_items) - 1)
                        c2.markdown(f"<div style='text-align:center'>{_fmt_date(theme_items[idx])}</div>",
                                    unsafe_allow_html=True)
                        entries = _parse_theme_entries(theme_items[idx].get("text", ""))
                        for e in entries:
                            st.markdown(f'<span style="color:#f85149;font-weight:bold">▷</span> {e}',
                                        unsafe_allow_html=True)
                        pass
                    else:
                        pass

        # ── 키워드 워드클라우드 ───────────────────────────────────────────────
        top_kw = _get_top_keywords(surge_items, n=8)
        if top_kw:
            _max = max(c for _, c in top_kw) if top_kw else 1
            _words_html = " &nbsp; ".join(
                f"<span style='font-size:{0.85 + 1.1*(c/_max):.2f}em;"
                f"color:rgba(21,101,192,{0.35+0.65*(c/_max):.2f});"
                f"font-weight:700;white-space:nowrap;'>{w}</span>"
                for w, c in top_kw
            )
            st.markdown(
                f"<div style='line-height:2.4;text-align:center;padding:6px 0;'>{_words_html}</div>",
                unsafe_allow_html=True)

            # 테마 순위 TOP 5 팝오버
            with st.popover("📊 테마 순위 TOP 5", use_container_width=True):
                _top5 = top_kw[:5]
                _medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
                _max5 = _top5[0][1] if _top5 else 1
                for _i, (_kw, _sc) in enumerate(_top5):
                    _bar = int(_sc / _max5 * 100)
                    st.markdown(
                        f"<div style='padding:6px 2px;'>"
                        f"<span style='font-size:1.0em;font-weight:700;'>{_medals[_i]} {_kw}</span>"
                        f"<div style='background:#21262d;border-radius:4px;height:6px;margin-top:4px;'>"
                        f"<div style='background:#1565C0;width:{_bar}%;height:6px;border-radius:4px;'></div>"
                        f"</div></div>", unsafe_allow_html=True)
                pass

        # ── 휴장일 ───────────────────────────────────────────────────────────
        st.markdown("### 🗓️ 휴장일", unsafe_allow_html=True)
        _hcol1, _hcol2 = st.columns(2)
        with _hcol1:
            with st.popover("🇰🇷 국내", use_container_width=True):
                st.info("🇰🇷 **코스피/코스닥 휴장일**")
                for h in _DOMESTIC_HOLIDAYS:
                    st.markdown(f"- **{h['date']}**: {h['reason']}")
        with _hcol2:
            with st.popover("🇺🇸 미국", use_container_width=True):
                st.warning("🇺🇸 **NYSE/NASDAQ 미국휴장일**")
                for h in _US_HOLIDAYS:
                    st.markdown(f"- **{h['date']}**: {h['reason']}")

        # ── 시스템 상태 ───────────────────────────────────────────────────────
        # P1 데이터 신선도
        meta = get_meta()
        exported_at = ""
        if meta:
            exported_at = meta.get("last_exported_at", "")
            try:
                age_min = (datetime.now() -
                           datetime.strptime(exported_at, "%Y-%m-%d %H:%M:%S")
                           ).total_seconds() / 60
                _p1_icon = "🟢" if age_min < 30 else "🟡" if age_min < 120 else "🔴"
            except:
                _p1_icon = "🟡"
        else:
            _p1_icon = "🔴"

        # 장 상태 (KST 기준)
        _now = datetime.utcnow() + timedelta(hours=9)
        _hm  = _now.hour * 60 + _now.minute
        _wd  = _now.weekday()
        if _wd >= 5:
            _mkt_icon, _mkt_label = "⚫", "휴장"
        elif 540 <= _hm < 930:
            _mkt_icon, _mkt_label = "🟢", "장중"
        elif 480 <= _hm < 540:
            _mkt_icon, _mkt_label = "🟡", "장전"
        elif 930 <= _hm < 1080:
            _mkt_icon, _mkt_label = "🟡", "시간외"
        else:
            _mkt_icon, _mkt_label = "⚫", "장외"

        # 뉴스 데이터 (infostock surge_items 유무)
        _news_icon = "🟢" if surge_items else "🔴"

        st.markdown(
            f"<div style='margin:14px 0 6px 0;padding-bottom:5px;border-bottom:1px solid #E8ECF4;'>"
            f"<strong>🛠 시스템 상태</strong>"
            f"&nbsp;&nbsp;{_p1_icon} {_mkt_icon} {_news_icon}"
            f"</div>"
            f"<div style='font-size:0.78em;color:#888;margin-bottom:8px;'>"
            f"수집: {exported_at[:16] if exported_at else '-'} &nbsp;·&nbsp; "
            f"장: {_mkt_label} &nbsp;·&nbsp; 차트: P1 OHLCV (30분 지연)"
            f"</div>",
            unsafe_allow_html=True)

        if st.button("🔄 데이터 새로고침", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    return "전체"


def render_p1_table(surge_table, rsi_snapshot, watchlist=None, market_filter="전체"):
    """Display P1 exported surge table — P1 스타일 재현 (pd.Styler + st.dataframe)."""
    df = pd.DataFrame(surge_table)
    if df.empty:
        return None

    # RSI signals — key format: {ticker}_daily / {ticker}_5m / {ticker}_1wk
    def _sig(t, suffix):
        d = rsi_snapshot.get(f"{t}{suffix}", {})
        return d.get("signal", "") if isinstance(d, dict) else ""
    df["신호"]    = df["종목코드"].apply(lambda t: _sig(t, "_daily"))
    df["단기신호"] = df["종목코드"].apply(lambda t: _sig(t, "_5m"))
    df["장기신호"] = df["종목코드"].apply(lambda t: _sig(t, "_1wk"))   # fix: was _weekly

    if "tier" in df.columns:
        df = df.rename(columns={"tier": "등급"})

    # 관심종목 마커
    _wl = set(str(c).zfill(6) for c in (watchlist or []))
    df["★"] = df["종목코드"].apply(lambda c: "★" if str(c).zfill(6) in _wl else "")
    df = df.sort_values("★", ascending=False, kind="stable")

    if market_filter != "전체":
        df = df[df["시장"] == market_filter]

    search = st.text_input("🔍 종목 검색", placeholder="종목명 또는 코드 입력",
                            label_visibility="collapsed")
    if search:
        mask = (df["종목명"].str.contains(search, na=False) |
                df["종목코드"].str.contains(search, na=False))
        df = df[mask]

    # 숫자 컬럼 numeric 보장
    for col in ["현재가", "당일등락", "7일누적", "거래대금"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    _ordered = ["★", "등급", "종목코드", "종목명", "시장", "테마", "신호", "단기신호",
                "현재가", "당일등락", "7일누적", "거래대금", "장기신호", "리스크"]
    df_raw = df[[c for c in _ordered if c in df.columns]].copy().reset_index(drop=True)

    # 시장 정보 보존 후 표시 컬럼에서 제거
    market_series = df_raw["시장"].copy() if "시장" in df_raw.columns else pd.Series([""] * len(df_raw))
    df_show = df_raw.drop(columns=["시장"], errors="ignore").copy()

    # ── Styler: 행 배경 (axis=1) ────────────────────────────────────────
    def _row_bg(row):
        star = str(row.get("★", ""))
        try: cum7 = float(df_raw.at[row.name, "7일누적"])
        except: cum7 = 0.0
        dim = cum7 < 40
        base = "font-size:12px;padding:2px 6px"
        if star == "★":
            bg = "#F0E8C0" if dim else "#FFFDE7"
            return [f"background-color:{bg};border-left:3px solid #F9A825;{base}"] * len(row)
        if dim:
            bg = "#E6E8EE" if row.name % 2 == 0 else "#ECEEF3"
            return [f"background-color:{bg};color:#999;{base}"] * len(row)
        bg = "#F5F7FA" if row.name % 2 == 0 else "#FFFFFF"
        return [f"background-color:{bg};{base}"] * len(row)

    # ── Styler: 종목명 색상 (시장별) ────────────────────────────────────
    def _name_color(row):
        styles = [""] * len(row)
        if "종목명" not in row.index:
            return styles
        mkt = market_series.at[row.name] if row.name in market_series.index else ""
        idx = list(row.index).index("종목명")
        if mkt == "KOSPI":
            styles[idx] = "color:#1565C0;font-weight:700;font-size:12px"
        elif mkt == "KOSDAQ":
            styles[idx] = "color:#BF360C;font-weight:700;font-size:12px"
        return styles

    # ── Styler: 당일등락 / 7일누적 색상 (axis=0, subset) ─────────────
    def _chg_colors(col_series):
        raw = df_raw.loc[col_series.index, col_series.name]
        return [
            f"color:{'#C62828' if v>0 else '#1565C0' if v<0 else '#555'};font-weight:700;font-size:12px"
            for v in raw
        ]

    # ── Styler: 등급 배경 (axis=0, subset) ────────────────────────────
    _tier_bg = {
        "👑S": "#FFF9C4", "🟢A": "#E8F5E9", "🟡B": "#FFF8E1",
        "🟠C": "#FFF3E0", "🔴D": "#FFEBEE",
    }
    _tier_color = {
        "👑S": "#F57F17", "🟢A": "#2E7D32", "🟡B": "#F57F17",
        "🟠C": "#E65100", "🔴D": "#B71C1C",
    }
    def _tier_style(col_series):
        styles = []
        for v in col_series:
            s = str(v)
            bg = _tier_bg.get(s, "")
            fg = _tier_color.get(s, "#333")
            if bg:
                styles.append(f"background-color:{bg};color:{fg};font-weight:700;font-size:11px;text-align:center;border-radius:3px")
            else:
                styles.append("font-size:11px;text-align:center")
        return styles

    styler = (
        df_show.style
        .apply(_row_bg,     axis=1)
        .apply(_name_color, axis=1)
    )
    if "등급" in df_show.columns:
        styler = styler.apply(_tier_style, subset=["등급"])
    if "당일등락" in df_show.columns:
        styler = styler.apply(_chg_colors, subset=["당일등락"])
    if "7일누적" in df_show.columns:
        styler = styler.apply(_chg_colors, subset=["7일누적"])

    # ── Styler: 테이블 전체 CSS (헤더 네이비, 컴팩트 패딩) ──────────────
    styler = styler.set_table_styles([
        {"selector": "thead th",
         "props": [("background-color", "#0F1C2E"), ("color", "#E8EDF3"),
                   ("font-size", "11px"), ("font-weight", "700"),
                   ("padding", "4px 6px"), ("border-bottom", "2px solid #2979FF"),
                   ("text-align", "center"), ("white-space", "nowrap")]},
        {"selector": "td",
         "props": [("padding", "2px 5px"), ("font-size", "12px"),
                   ("border-bottom", "1px solid #E8ECF0"), ("white-space", "nowrap")]},
        {"selector": "table",
         "props": [("border-collapse", "collapse"), ("width", "100%")]},
    ])

    # ── Styler: 숫자 포맷 ────────────────────────────────────────────
    fmt = {}
    if "현재가"  in df_show.columns:
        fmt["현재가"]  = lambda v: f"{int(v):,}" if v else "-"
    if "당일등락" in df_show.columns:
        fmt["당일등락"] = lambda v: f"{'+' if v>=0 else ''}{v:.2f}%"
    if "7일누적" in df_show.columns:
        fmt["7일누적"] = lambda v: f"{v:.2f}%"
    if "거래대금" in df_show.columns:
        def _amt(v):
            if v >= 10000: return f"{v/10000:.1f}조"
            if v >= 1000:  return f"{v/1000:.1f}천억"
            return f"{v:.0f}억"
        fmt["거래대금"] = _amt
    if fmt:
        styler = styler.format(fmt)

    col_cfg = {
        "★":     st.column_config.TextColumn(width="small"),
        "등급":   st.column_config.TextColumn(width="small"),
        "종목코드": st.column_config.TextColumn("코드", width="small"),
        "종목명":  st.column_config.TextColumn(width="medium"),
        "테마":   st.column_config.TextColumn(width="small"),
        "신호":   st.column_config.TextColumn(width="small"),
        "단기신호": st.column_config.TextColumn("단기", width="small"),
        "현재가":  st.column_config.TextColumn(width="small"),
        "당일등락": st.column_config.TextColumn("당일", width="small"),
        "7일누적": st.column_config.TextColumn("7일", width="small"),
        "거래대금": st.column_config.TextColumn(width="small"),
        "장기신호": st.column_config.TextColumn("장기", width="small"),
        "리스크":  st.column_config.TextColumn(width="small"),
    }

    selected = st.dataframe(
        styler, use_container_width=True, hide_index=True,
        on_select="rerun", selection_mode="single-row",
        column_config=col_cfg, height=460,
    )
    return selected, df_raw


def render_watchlist_table(wdf):
    disp = _make_display_df(wdf)
    col_cfg = {
        "등락률(%)": st.column_config.NumberColumn(format="%.2f%%", width="small"),
        "거래량":    st.column_config.NumberColumn(format="%d"),
    }
    selected = st.dataframe(disp, use_container_width=True, hide_index=True,
                            on_select="rerun", selection_mode="single-row",
                            column_config=col_cfg)
    return selected, wdf


def render_detail(ticker, name, rsi_snapshot, cb_overhang, surge_reasons=None):
    st.divider()

    # ── 공통 헬퍼 ────────────────────────────────────────────────────────────
    def _fmt_cap(v):
        try:
            n = float(v)
            if n >= 1e12: return f"{n/1e12:.1f}조원"
            if n >= 1e8:  return f"{n/1e8:.0f}억원"
            return f"{n:,.0f}원"
        except: return "N/A"

    def _mrow(label, val, bdg=""):
        return (f"<div style='display:flex;justify-content:space-between;align-items:center;"
                f"padding:5px 0;border-bottom:1px solid #f0f0f0;font-size:0.88em;'>"
                f"<span style='color:#666;'>{label}</span>"
                f"<span style='font-weight:600;'>{val}&nbsp;{bdg}</span></div>")

    def _badge(text, kind):
        c  = {"good":"#2e7d32","warn":"#e65100","bad":"#c62828","neutral":"#666"}.get(kind,"#666")
        bg = {"good":"#e8f5e9","warn":"#fff3e0","bad":"#ffebee","neutral":"#f5f5f5"}.get(kind,"#f5f5f5")
        return (f"<span style='background:{bg};color:{c};border-radius:4px;"
                f"padding:1px 6px;font-size:0.8em;font-weight:700;'>{text}</span>")

    # ── 데이터 수집 ──────────────────────────────────────────────────────────
    corp  = get_corp_info(ticker)
    ohlcv = get_ohlcv(ticker)
    _st   = get_surge_table()
    _row  = next((r for r in _st
                  if str(r.get("종목코드","")).zfill(6) == str(ticker).zfill(6)), {})

    cur_price  = float(_row.get("현재가",0) or corp.get("현재가",0) or 0)
    if cur_price == 0 and not ohlcv.empty:
        try: cur_price = float(ohlcv["Close"].iloc[-1])
        except: pass

    daily_chg  = float(_row.get("당일등락", 0) or 0)
    week_chg   = float(_row.get("7일누적",  0) or 0)
    mktcap_str = _fmt_cap(corp.get("시가총액") or _row.get("시가총액") or 0)

    chg_color  = "#c62828" if daily_chg >= 0 else "#1565c0"
    chg_sign   = "▲"       if daily_chg >= 0 else "▼"
    week_color = "#c62828" if week_chg  >= 0 else "#1565c0"
    week_sign  = "▲"       if week_chg  >= 0 else "▼"

    outline     = corp.get("outline", {})
    # KIS 재무 데이터 우선, 없으면 공공데이터 fallback
    # kis_fin_ratio: 5년치 list (PER 밴드 계산용) — 팝오버에는 [0]만 사용
    _kis_ratio_raw = corp.get("kis_fin_ratio", [])
    if isinstance(_kis_ratio_raw, list):
        kis_ratio    = _kis_ratio_raw                          # valuation용 list
        _latest_ratio = _kis_ratio_raw[0] if _kis_ratio_raw else {}
    elif isinstance(_kis_ratio_raw, dict) and _kis_ratio_raw:
        kis_ratio    = [_kis_ratio_raw]
        _latest_ratio = _kis_ratio_raw
    else:
        kis_ratio    = []
        _latest_ratio = {}
    kis_stab    = corp.get("kis_stability", {})   or {}
    kis_income  = corp.get("kis_income", {})      or {}
    kis_balance = corp.get("kis_balance", {})     or {}
    kis_basic   = corp.get("kis_basic", {})       or {}
    fin_pub     = corp.get("financial_pub", {})   or corp.get("financial", {})

    # ── 헤더 (full width) ────────────────────────────────────────────────────
    _hdr_col, _nv_col = st.columns([5, 1])
    _hdr_col.markdown(f"### 🛡️ {name} ({ticker}) 분석 리포트")
    _nv_col.link_button("🌐 네이버 종합분석 확인",
                        f"https://finance.naver.com/item/main.naver?code={ticker}",
                        use_container_width=True)

    # ── 1:2 컬럼 ─────────────────────────────────────────────────────────────
    col_left, col_right = st.columns([1, 2])

    # ════════════════════════════════════════════════════════════════════════
    # 왼쪽 패널
    # ════════════════════════════════════════════════════════════════════════
    with col_left:

        # ① 핵심 요약 카드
        _sic   = outline.get("sicNm", "") or kis_basic.get("bstp_kor_isnm", "")
        _estb  = outline.get("enpEstbDt", "")
        _estb_fmt = f"{_estb[:4]}.{_estb[4:6]}.{_estb[6:]}" if len(_estb) == 8 else _estb
        _emp   = outline.get("enpEmpeCnt", "")
        _meta_row = ""
        if _sic or _estb_fmt or _emp:
            _parts = []
            if _sic:     _parts.append(f"업종: {_sic}")
            if _estb_fmt:_parts.append(f"설립: {_estb_fmt}")
            if _emp:     _parts.append(f"직원: {_emp}명")
            _meta_row = (f"<div style='font-size:0.75em;color:#999;margin-top:6px;'>"
                         f"{' &nbsp;·&nbsp; '.join(_parts)}</div>")
        st.markdown(
            f"<div style='background:#f8f9fa;border-radius:12px;"
            f"padding:16px 20px;margin-bottom:8px;'>"
            f"<div style='display:flex;align-items:flex-end;gap:20px;flex-wrap:wrap;'>"
            f"<div><div style='font-size:0.75em;color:#999;'>시가총액</div>"
            f"<div style='font-size:1.5em;font-weight:700;'>{mktcap_str}</div></div>"
            f"<div><div style='font-size:0.75em;color:#999;'>현재가</div>"
            f"<div style='font-size:1.5em;font-weight:700;'>{int(cur_price):,}원 "
            f"<span style='font-size:0.55em;color:{chg_color};'>"
            f"{chg_sign}{abs(daily_chg):.2f}%</span></div></div>"
            f"<div><div style='font-size:0.75em;color:#999;'>7일 누적</div>"
            f"<div style='font-size:1.2em;font-weight:600;color:{week_color};'>"
            f"{week_sign}{abs(week_chg):.1f}%</div></div>"
            f"</div>{_meta_row}</div>",
            unsafe_allow_html=True)

        # ② 팝업 버튼 2개 — P1 ui_detail.py와 동일한 로직
        _pc1, _pc2 = st.columns(2)
        with _pc1:
            with st.popover("10초 재무 확인", use_container_width=True):
                st.markdown("**🛡️ 재무 안정성 지표**")
                # 유동비율 / 부채비율 (kis_stability)
                crnt_rate = float(kis_stab.get("crnt_rate", 0) or 0)
                lblt_rate = float(kis_stab.get("lblt_rate", 0) or 0)
                _crnt_str = f"{crnt_rate:.1f}%" if crnt_rate > 0 else "N/A"
                _lblt_str = f"{lblt_rate:.1f}%" if lblt_rate > 0 else "N/A"
                _crnt_kind = "good" if crnt_rate >= 150 else ("warn" if crnt_rate >= 100 else "bad")
                _lblt_kind = "good" if 0 < lblt_rate <= 100 else ("warn" if lblt_rate <= 200 else "bad")
                # 자본잠식 (kis_balance: cpfn / total_cptl) — P1과 동일 공식
                _cpfn = float(kis_balance.get("cpfn", 0) or 0)
                _cptl = float(kis_balance.get("total_cptl", 0) or 0)
                if _cpfn > 0 and _cptl <= 0:
                    _cap_label = "완전잠식"; _cap_kind = "bad"
                elif _cpfn > 0 and _cptl < _cpfn:
                    _cap_pct   = (_cpfn - _cptl) / _cpfn * 100
                    _cap_label = f"부분잠식 {_cap_pct:.1f}%"; _cap_kind = "warn"
                elif _cptl > 0:
                    _cap_label = "정상"; _cap_kind = "good"
                else:
                    _cap_label = "N/A"; _cap_kind = "neutral"
                st.markdown(
                    _mrow("유동비율", _crnt_str,
                          _badge("양호" if crnt_rate >= 150 else ("주의" if crnt_rate >= 100 else "위험"), _crnt_kind)) +
                    _mrow("부채비율", _lblt_str,
                          _badge("양호" if 0 < lblt_rate <= 100 else ("주의" if lblt_rate <= 200 else "위험"), _lblt_kind)) +
                    _mrow("자본잠식", _cap_label, _badge(_cap_label.split()[0], _cap_kind)),
                    unsafe_allow_html=True)

        with _pc2:
            with st.popover("핵심 투자 지표", use_container_width=True):
                st.markdown("**📌 성장 및 가치 지표**")
                # P1과 동일: financial_ratio[0]에서 eps/bps/roe_val/bsop_prfi_inrt/grs
                _eps_val = float(_latest_ratio.get("eps", 0) or 0)
                _bps_val = float(_latest_ratio.get("bps", 0) or 0)
                _roe     = float(_latest_ratio.get("roe_val", 0) or 0)
                _opm     = float(_latest_ratio.get("bsop_prfi_inrt", 0) or 0)
                _grs     = float(_latest_ratio.get("grs", 0) or 0)
                # PER / PBR: 현재가 / eps,bps 직접 계산 (P1 방식)
                if _eps_val > 0:
                    per_str = f"{cur_price / _eps_val:.2f}배"
                elif _eps_val < 0:
                    per_str = "적자"
                else:
                    per_str = "N/A"
                pbr_str = f"{cur_price / _bps_val:.2f}배" if _bps_val > 0 else "N/A"
                eps_str = f"{_eps_val:,.0f}원" if _eps_val != 0 else "N/A"
                def _per_badge(s):
                    if "N/A" in s: return _badge("N/A", "neutral")
                    if "적자" in s: return _badge("적자", "bad")
                    try:
                        v = float(s.replace("배","").strip())
                        return _badge("양호" if v<=15 else ("주의" if v<=30 else "과열"),
                                      "good" if v<=15 else ("warn" if v<=30 else "bad"))
                    except: return _badge("?", "neutral")
                def _pbr_badge(s):
                    if "N/A" in s: return _badge("N/A", "neutral")
                    try:
                        v = float(s.replace("배","").strip())
                        return _badge("양호" if v<=2 else "주의", "good" if v<=2 else "warn")
                    except: return _badge("?", "neutral")
                st.markdown(
                    _mrow("PER", per_str, _per_badge(per_str)) +
                    _mrow("PBR", pbr_str, _pbr_badge(pbr_str)) +
                    _mrow("EPS", eps_str,
                          _badge("양호","good") if _eps_val > 0 else (_badge("적자","bad") if _eps_val < 0 else _badge("N/A","neutral"))) +
                    _mrow("ROE", f"{_roe:.1f}%" if _roe != 0 else "N/A",
                          _badge("우수","good") if _roe>=15 else (_badge("양호","good") if _roe>=8 else
                          (_badge("주의","warn") if _roe>0 else (_badge("위험","bad") if _roe<0 else _badge("N/A","neutral"))))) +
                    _mrow("영업이익률", f"{_opm:.1f}%" if _opm != 0 else "N/A",
                          _badge("양호","good") if _opm>=10 else (_badge("보통","neutral") if _opm>=0 else _badge("위험","bad"))) +
                    _mrow("매출성장률", f"{_grs:.1f}%" if _grs != 0 else "N/A",
                          _badge("성장","good") if _grs>=10 else (_badge("보통","neutral") if _grs>=0 else _badge("역성장","bad"))),
                    unsafe_allow_html=True)

        st.divider()

        # ③ 급등 사유 히스토리 — P1 ui_detail.py L358-396 동일 구조
        # accumulated.json에서 해당 종목 히스토리 필터 (P1 get_surge_reason_for_ticker 역할)
        _acc = _github_json("data/accumulated.json") or {}
        _acc_items = _acc.get("items", []) if isinstance(_acc, dict) else []
        _hist = []
        _pat  = f"({ticker})"
        for _it in _acc_items:
            if _it.get("category","") != "특징 상한가 및 급등종목": continue
            _txt = _it.get("text","")
            if _pat not in _txt: continue
            _entries = _parse_body_entries(_txt)
            for _e in _entries:
                if str(_e.get("code","")) == str(ticker):
                    _d = _it.get("sendDate","")
                    if len(_d)==8: _d = f"{_d[:4]}.{_d[4:6]}.{_d[6:]}"
                    _hist.append({"date": _d, "reason": _e.get("reason","")})

        st.markdown(
            f"#### 🚨 급등 사유 히스토리"
            f"<span style='font-size:0.6em;font-weight:normal;color:gray;'>"
            f"&nbsp;(누적 {len(_hist)}건)</span>",
            unsafe_allow_html=True,
        )
        if len(_hist) == 0:
            st.info(f"ℹ️ '{name}' 관련 특징주 뉴스가 없습니다.")
        else:
            for _h in _hist[:7]:
                st.markdown(
                    f"<div style='background:#e8f4fd;border-left:3px solid #1976d2;"
                    f"padding:6px 12px;border-radius:4px;margin:4px 0;font-size:0.88em;'>"
                    f"<span style='color:#888;font-size:0.85em;'>{_h['date']}</span>"
                    f"&nbsp;›&nbsp;{_h['reason']}</div>",
                    unsafe_allow_html=True)

        st.divider()

        # ④ 잠재적 매도 물량 (CB/BW) — P1 ui_detail.py L399-475 동일 로직
        with st.container(border=True):
            st.markdown("#### 🕵️‍♂️ 잠재적 매도 물량 (미상환 사채 5년)")
            _cb_balance = cb_overhang.get(str(ticker).zfill(6)) or cb_overhang.get(ticker) or []
            _active_cb = [b for b in _cb_balance
                          if not b.get("is_expired") and b.get("unredeemed", 0) > 0]
            if not _cb_balance:
                st.info("ℹ️ 최근 5년 내 CB/BW 발행 공시가 없습니다.")
            elif not _active_cb:
                st.success("✅ 조회된 CB/BW 전량 만기상환 또는 완전 전환 완료")
            else:
                # 발행주식수 (희석률 계산용)
                _issued_shares = 0
                try:
                    _issued_shares = int(
                        str(kis_basic.get("lstg_stqt", "0")).replace(",", "")
                    )
                except Exception:
                    pass
                _total_conv_shares = sum(b.get("convertible_shares", 0) for b in _active_cb)
                _total_unredeemed  = sum(b.get("unredeemed", 0) for b in _active_cb)
                _dilution = (_total_conv_shares / _issued_shares * 100) if _issued_shares > 0 else 0
                if _dilution >= 20:
                    st.error(f"🚨 **미전환잔액 {_total_unredeemed/1e8:.1f}억원 | 전환가능주식수 {_total_conv_shares:,}주 | 희석률 {_dilution:.1f}%**")
                elif _dilution >= 5:
                    st.warning(f"⚠️ **미전환잔액 {_total_unredeemed/1e8:.1f}억원 | 전환가능주식수 {_total_conv_shares:,}주 | 희석률 {_dilution:.1f}%**")
                else:
                    st.info(f"ℹ️ 미전환잔액 {_total_unredeemed/1e8:.1f}억원 | 전환가능주식수 {_total_conv_shares:,}주 | 희석률 {_dilution:.1f}%")

                _rows_cb = []
                _danger_series, _warn_series = [], []
                for b in _cb_balance:
                    _cp        = b.get("conversion_period", "")
                    _price     = b.get("conversion_price", 0)
                    _in_period = _period_contains_today(_cp)
                    _no_period = not _cp or _cp == "-"
                    _under_mkt = _price > 0 and cur_price > 0 and _price < cur_price
                    _over_mkt  = _price > 0 and cur_price > 0 and _price >= cur_price
                    _is_danger = not b.get("is_expired") and _under_mkt and (_in_period or _no_period)
                    _is_warn   = not b.get("is_expired") and _in_period and _over_mkt
                    _label = f"{b.get('series', '-')}회차"
                    if _is_danger:
                        _label = f"🔴 {_label}"; _danger_series.append((b.get("series","-"), _price))
                    elif _is_warn:
                        _label = f"🟡 {_label}"; _warn_series.append((b.get("series","-"), _price))
                    _rows_cb.append({
                        "회차":           _label,
                        "종류":           "전환사채" if "전환" in b.get("bond_type", "") else "BW",
                        "미전환잔액(억)":  f"{b.get('unredeemed', 0)/1e8:.1f}",
                        "전환가액(원)":    f"{_price:,}",
                        "전환가능주식수":  f"{b.get('convertible_shares', 0):,}",
                        "전환가능기간":   _cp if _cp else "-",
                        "데이터 출처":    b.get("source", ""),
                    })
                if _danger_series:
                    _danger_txt = ", ".join(f"{s}회차(전환가:{p:,}원)" for s, p in _danger_series)
                    st.error(
                        f"🔴 **전환 리스크: {_danger_txt}** — "
                        f"전환가액이 현재가({int(cur_price):,}원)보다 낮음 (전환청구기간 미확인 회차 포함)"
                    )
                if _warn_series:
                    _warn_txt = ", ".join(f"{s}회차(전환가:{p:,}원)" for s, p in _warn_series)
                    st.warning(
                        f"🟡 **{_warn_txt}** — "
                        f"전환청구기간 내이나 현재가({int(cur_price):,}원)가 전환가액보다 낮습니다."
                    )
                st.dataframe(pd.DataFrame(_rows_cb), use_container_width=True, hide_index=True)

            with st.expander("🛠️ [참고] DART 공시 원문 목록"):
                _ditems = get_dart(ticker)
                if _ditems:
                    for _di in _ditems[:10]:
                        _rno = _di.get("rcept_no", "")
                        _url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={_rno}" if _rno else ""
                        _lbl = f"**[{_di.get('rcept_dt', '')[:8]}]** {_di.get('report_nm', '')}"
                        if _url:
                            st.markdown(f"{_lbl}  \n[공시원문]({_url})")
                        else:
                            st.markdown(_lbl)
                else:
                    st.write("공시 없음")

        st.divider()

        # ⑤ 감사보고서 / 상호변경
        st.markdown("#### 📋 감사보고서 / 상호변경")
        _gov = corp.get("gov_risk") if isinstance(corp.get("gov_risk"), dict) else None
        if _gov:
            _bad_audit        = _gov.get("bad_audit", False)
            _audit_details    = _gov.get("audit_details", "확인 불가")
            _name_change_cnt  = _gov.get("name_change_count", 0)
            _audit_rcept_no   = _gov.get("audit_rcept_no", "")
            _gc1              = _gov.get("gc1", False)
            _gc2              = _gov.get("gc2", False)
            _ic               = _gov.get("ic", False)

            # Row 1: 감사의견 | 계속기업 존속불확실성 사유
            _r1c1, _r1c2 = st.columns(2)
            if not _bad_audit:
                _r1c1.success(f"✅ 감사의견: {_audit_details}")
            else:
                _r1c1.error(f"🚨 감사의견: {_audit_details}")
                if _audit_rcept_no:
                    _r1c1.link_button(
                        "📄 DART 감사보고서 열람",
                        f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={_audit_rcept_no}")
                else:
                    _r1c1.caption("감사보고서 접수번호를 DART에서 찾을 수 없습니다.")
            if _gc1:
                _r1c2.warning("⚠️ 계속기업 존속불확실성 사유 : 해당")
            else:
                _r1c2.success("✅ 계속기업 존속불확실성 사유 : 미해당")

            # Row 2: 감사 외 계속기업 | 내부회계관리제도
            _r2c1, _r2c2 = st.columns(2)
            if _gc2:
                _r2c1.warning("⚠️ 감사 외 계속기업 존속불확실성 : 기재")
            else:
                _r2c1.success("✅ 감사 외 계속기업 존속불확실성 : 미기재")
            if _ic:
                _r2c2.warning("⚠️ 내부회계관리제도 감사의견 : 해당")
            else:
                _r2c2.success("✅ 내부회계관리제도 감사의견 : 미해당")

            # Row 3: 상호변경
            _r3c1, _ = st.columns(2)
            if _name_change_cnt == 0:
                _r3c1.success("✅ 상호변경: 없음")
            else:
                _past = _gov.get("past_names", "")
                _r3c1.warning(f"⚠️ 상호변경: {_name_change_cnt}회 ({_past})")
        else:
            st.info("ℹ️ 감사/상호변경 데이터 미수집 — Full Export 후 표시됩니다.")

        st.divider()

        # ⑥ 밸류에이션 (Audited PER Band) — P1 ui_detail.py 동일 알고리즘
        st.markdown("#### 📈 밸류에이션 (Audited)")
        with st.container(border=True):
            # kis_fin_ratio: 5년치 리스트 (export_to_github Full Export 기준)
            _fin_list = kis_ratio if isinstance(kis_ratio, list) else (
                        [kis_ratio] if isinstance(kis_ratio, dict) and kis_ratio else [])

            _latest_fin  = _fin_list[0] if _fin_list else {}
            _eps_base    = 0.0
            try: _eps_base = float(_latest_fin.get("eps", 0) or 0)
            except Exception: pass

            # 연도별 PER 밴드 계산
            _year_bands = []
            if _eps_base > 0 and not ohlcv.empty:
                for _vr in _fin_list[:5]:
                    try:
                        _veps = float(_vr.get("eps", 0) or 0)
                        _vym  = str(_vr.get("stac_yymm", ""))
                        if _veps <= 0 or len(_vym) < 6:
                            continue
                        _vyear = int(_vym[:4])
                        _vmon  = int(_vym[4:6])
                        _vys   = pd.Timestamp(year=_vyear, month=1, day=1)
                        _vye   = (pd.Timestamp(year=_vyear, month=_vmon, day=1)
                                  + pd.offsets.MonthEnd(0))
                        _vslice = ohlcv[(ohlcv.index >= _vys) & (ohlcv.index <= _vye)]
                        if _vslice.empty:
                            continue
                        _vph = float(_vslice["High"].max()) / _veps
                        _vpl = float(_vslice["Low"].min())  / _veps
                        if 0 < _vpl < _vph < 500:
                            _year_bands.append({
                                "year": _vyear, "per_h": _vph, "per_l": _vpl,
                                "year_h": float(_vslice["High"].max()),
                                "year_l": float(_vslice["Low"].min()),
                            })
                    except Exception:
                        continue

            # 중앙값 PER 기반 밴드 산출
            _val_ok = False
            h_band = l_band = 0.0
            _med_ph = _med_pl = 0.0
            _used_years = []
            if _eps_base > 0 and _year_bands:
                _sph = sorted(b["per_h"] for b in _year_bands)
                _spl = sorted(b["per_l"] for b in _year_bands)
                _n   = len(_sph)
                _med_ph = (_sph[_n//2] if _n % 2 == 1
                           else (_sph[_n//2-1] + _sph[_n//2]) / 2)
                _med_pl = (_spl[_n//2] if _n % 2 == 1
                           else (_spl[_n//2-1] + _spl[_n//2]) / 2)
                h_band = _eps_base * _med_ph
                l_band = _eps_base * _med_pl
                _used_years = [str(b["year"]) for b in _year_bands]
                _val_ok = True

            # 렌더링
            if _val_ok:
                if cur_price < l_band:
                    st.success(f"🛡️ **강력 저평가** (매수 유리) — 현재가가 밴드 하단({int(l_band):,}원) 아래입니다.")
                elif l_band <= cur_price <= h_band:
                    st.info(f"✅ **적정 주가** (보유/관찰) — 주가가 역사적 밴드({int(l_band):,} ~ {int(h_band):,}원) 내에 있습니다.")
                else:
                    st.warning(f"⚠️ **고평가 주의** — 현재가가 밴드 상단({int(h_band):,}원) 위에 있습니다.")
                if h_band > l_band:
                    _pos = (cur_price - l_band) / (h_band - l_band) * 100
                    st.progress(max(0, min(100, int(_pos))),
                                text=f"현재 주가 위치 ({int(_pos)}%)")
                st.markdown(
                    f"**Max Value:** {int(h_band):,}원 &nbsp;&nbsp;|&nbsp;&nbsp; **Min Value:** {int(l_band):,}원",
                    unsafe_allow_html=True)
                with st.expander(f"📊 연도별 PER 밴드 상세 ({', '.join(_used_years)})"):
                    _rows_val = []
                    for _b in _year_bands:
                        _rows_val.append({
                            "기준연도":        str(_b["year"]),
                            "고가PER":        f"{_b['per_h']:.1f}배",
                            "저가PER":        f"{_b['per_l']:.1f}배",
                            "현EPS 적용 상단": f"{int(_eps_base * _b['per_h']):,}원",
                            "현EPS 적용 하단": f"{int(_eps_base * _b['per_l']):,}원",
                        })
                    st.dataframe(pd.DataFrame(_rows_val),
                                 use_container_width=True, hide_index=True)
                    st.caption(
                        f"※ 중앙값 PER 사용: 고가PER {_med_ph:.1f}배 / "
                        f"저가PER {_med_pl:.1f}배 × 최신 EPS {int(_eps_base):,}원")
            else:
                st.error("🚨 이익 적자: 가치 산출 불가 (Valuation Skip)")
                st.caption("💡 본 종목은 현재 적자 상태이거나 유의미한 이익 데이터를 공시하지 않았습니다.")

    # ════════════════════════════════════════════════════════════════════════
    # 오른쪽: 캔들 차트
    # ════════════════════════════════════════════════════════════════════════
    with col_right:
        st.markdown("### 📈 일봉 캔들 차트")
        render_chart(ticker, name)


_KW_STOP = {"및", "의", "을", "를", "에", "이", "가", "은", "는", "로", "으로",
            "에서", "과", "와", "도", "등", "한", "하는", "하여", "대한", "위한",
            "관련", "통해", "따른", "대해", "통한", "있는", "없는", "위해", "향한"}

def _top_keywords(news_items, top_n=15):
    words = []
    for it in news_items:
        tokens = re.findall(r'[가-힣]{2,}', it.get("title", ""))
        words.extend(t for t in tokens if t not in _KW_STOP)
    return Counter(words).most_common(top_n)


def render_news(news_items):
    if not news_items:
        st.info("테마뉴스 없음. Project 1 데몬이 실행 중인지 확인하세요.")
        return

    # Hot keyword tags
    top_kws = _top_keywords(news_items)
    if top_kws:
        tags_html = " ".join(
            f"<span style='background:#1f3a5f;color:#58a6ff;border-radius:12px;"
            f"padding:3px 10px;font-size:0.82em;margin:2px;display:inline-block'>"
            f"{w} <span style='color:#8b949e'>{c}</span></span>"
            for w, c in top_kws)
        st.markdown(f"**🔥 핫 키워드**<br>{tags_html}", unsafe_allow_html=True)
        st.markdown("")

    kw = st.text_input("🔍 키워드 필터", placeholder="예: 반도체, AI, 바이오",
                        label_visibility="collapsed")
    filtered = news_items
    if kw:
        kw_lower = kw.lower()
        filtered = [it for it in news_items
                    if kw_lower in str(it.get("title", "")).lower()
                    or kw_lower in str(it.get("text") or it.get("content") or it.get("body", "")).lower()]
    st.caption(f"총 {len(filtered)}건")
    for item in filtered[:30]:
        title = item.get("title", "")
        body  = item.get("text") or item.get("content") or item.get("body", "")
        dt    = str(item.get("sendDate") or item.get("date", ""))[:10]
        with st.expander(f"[{dt}] {title}"):
            if body:
                st.markdown(str(body)[:600] + ("..." if len(str(body)) > 600 else ""))


def main():
    st.title("📈 급등주 모멘텀 대시보드 (공개)")
    st.caption("P1 수집 → GitHub JSON → P2 표시 (단방향 흐름)")

    for key, val in [("sel_ticker_surge", ""), ("sel_name_surge", ""),
                     ("sel_ticker_watch", ""), ("sel_name_watch", "")]:
        if key not in st.session_state:
            st.session_state[key] = val

    indices_p1      = get_indices_p1()
    indices_history = get_indices_history()
    surge_table     = get_surge_table()
    surge_history   = get_surge_history()
    surge_reasons   = get_surge_reasons()
    rsi_snapshot    = get_rsi_snapshot()
    cb_overhang     = get_cb_overhang()
    watchlist       = get_watchlist()
    surge_items, theme_items = get_accumulated_news()

    market_filter = render_sidebar(indices_p1, surge_items, theme_items, indices_history)

    tab1, tab2, tab3 = st.tabs(["🚀 급등주 랭킹", "⭐ 관심종목", "🔍 종목검색"])

    with tab1:
        # ── 날짜 셀렉터 ─────────────────────────────────────────────────────
        _today_label = "오늘 (실시간)"
        _hist_labels = []
        for _snap in surge_history:
            _d = _snap.get("date", "")
            if len(_d) == 8:
                _hist_labels.append(f"{_d[:4]}.{_d[4:6]}.{_d[6:]}")
            elif _d:
                _hist_labels.append(_d)
        _date_options = [_today_label] + _hist_labels
        _sel_date = st.radio(
            "📅 날짜 선택", _date_options,
            horizontal=True, label_visibility="collapsed",
            key="surge_date_sel",
        ) if len(_date_options) > 1 else _today_label

        # 날짜에 따라 표시할 데이터 결정
        if _sel_date == _today_label or not _hist_labels:
            _display_surge = surge_table
            _display_label = "P1 실시간 데이터"
            _meta = get_meta()
            _upd  = _meta.get("updated_at", "")[:16] if _meta else ""
        else:
            _hist_idx = _hist_labels.index(_sel_date)
            _display_surge = surge_history[_hist_idx].get("data", []) if _hist_idx < len(surge_history) else []
            _display_label = f"{_sel_date} 스냅샷"
            _upd = _sel_date

        if _display_surge:
            _cnt  = len(_display_surge)
            st.markdown(
                f"""<div style='
                    background:linear-gradient(90deg,#0F1C2E 0%,#1A2E46 100%);
                    border-left:4px solid #2979FF;
                    border-radius:0 6px 6px 0;
                    padding:8px 16px; margin-bottom:6px;
                    display:flex; align-items:center; justify-content:space-between;'>
                  <span style='color:#E8EDF3;font-size:1.0em;font-weight:700;letter-spacing:0.5px;'>
                    🚀&nbsp;급등주 랭킹 — {_display_label}
                  </span>
                  <span style='color:#90CAF9;font-size:0.8em;'>
                    {_cnt}종목&nbsp;&nbsp;|&nbsp;&nbsp;업데이트&nbsp;{_upd}
                  </span>
                </div>""",
                unsafe_allow_html=True)
            result = render_p1_table(_display_surge, rsi_snapshot, watchlist, market_filter)
            if result:
                selected, df_p1 = result
                rows = selected.selection.rows if hasattr(selected, "selection") else []
                if rows:
                    row = df_p1.iloc[rows[0]]
                    st.session_state.sel_ticker_surge = str(row["종목코드"]).zfill(6)
                    st.session_state.sel_name_surge   = row["종목명"]

            # ── 관심종목 토글 버튼 (종목 선택 시 표시) ──────────────────────
            _stk = st.session_state.sel_ticker_surge
            _snm = st.session_state.sel_name_surge
            if _stk:
                _wl_set   = set(str(c).zfill(6) for c in watchlist)
                _in_wl    = _stk in _wl_set
                _btn_lbl  = f"★ 관심 해제  [{_snm}]" if _in_wl else f"☆ 관심 추가  [{_snm}]"
                _btn_type = "secondary" if _in_wl else "primary"
                if st.button(_btn_lbl, type=_btn_type, key="wl_toggle_surge"):
                    _wl_list = list(watchlist)
                    if _in_wl:
                        _wl_list = [c for c in _wl_list if str(c).zfill(6) != _stk]
                    else:
                        _wl_list.append(_stk)
                    if _github_put("data/watchlist.json",
                                   json.dumps(_wl_list, ensure_ascii=False)):
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("GitHub 업로드 실패")

                render_detail(_stk, _snm, rsi_snapshot, cb_overhang, surge_reasons)
        else:
            st.info("급등주 데이터가 없습니다. P1 데몬이 실행 중인지 확인하세요.")

    with tab2:
        st.markdown("<div class='section-title'>⭐ 관심종목 - 전일 종가 기준 (T+1)</div>",
                    unsafe_allow_html=True)

        # Watchlist management UI
        with st.expander("➕ 관심종목 관리", expanded=False):
            col_inp, col_btn = st.columns([3, 1])
            with col_inp:
                new_cd = st.text_input("종목코드 (6자리)", max_chars=6,
                                       placeholder="005930", label_visibility="collapsed")
            with col_btn:
                if st.button("추가", use_container_width=True):
                    cd = str(new_cd).strip().zfill(6)
                    if cd and cd not in watchlist:
                        watchlist.append(cd)
                        if _github_put("data/watchlist.json",
                                       json.dumps(watchlist, ensure_ascii=False)):
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error("GitHub 업로드 실패")
            if watchlist:
                # Build code→name map from surge_table first, then raw API fallback
                _name_map = {str(r.get("종목코드", "")).zfill(6): r.get("종목명", "")
                             for r in surge_table if r.get("종목코드")}
                _missing = [c for c in watchlist if not _name_map.get(str(c).zfill(6))]
                if _missing:
                    # watchlist_prices.json에서 종목명 보완
                    _wp = _github_json("data/watchlist_prices.json") or {}
                    for _mc in _missing:
                        _nm = (_wp.get(str(_mc).zfill(6)) or {}).get("itmsNm", "")
                        if _nm:
                            _name_map[str(_mc).zfill(6)] = _nm
                st.caption("현재 관심종목 (X 클릭하면 제거)")
                rm_cols = st.columns(min(len(watchlist), 5))
                for i, cd in enumerate(watchlist):
                    _label = _name_map.get(str(cd).zfill(6), "") or cd
                    with rm_cols[i % 5]:
                        if st.button(f"{_label} ✕", key=f"rm_{cd}",
                                     use_container_width=True):
                            watchlist.remove(cd)
                            _github_put("data/watchlist.json",
                                        json.dumps(watchlist, ensure_ascii=False))
                            st.cache_data.clear()
                            st.rerun()

        if not watchlist:
            st.info("관심종목 없음. 위 관리 패널에서 종목코드를 추가하세요.")
        else:
            wdf = get_watchlist_prices(watchlist)
            if wdf.empty:
                st.warning("관심종목 시세 조회 실패.")
            else:
                result_w = render_watchlist_table(wdf)
                if result_w:
                    selected_w, wdf2 = result_w
                    rows_w = selected_w.selection.rows if hasattr(selected_w, "selection") else []
                    if rows_w:
                        row_w = wdf2.iloc[rows_w[0]]
                        st.session_state.sel_ticker_watch = str(row_w["srtnCd"]).zfill(6)
                        st.session_state.sel_name_watch   = row_w["itmsNm"]
                    if st.session_state.sel_ticker_watch:
                        render_detail(st.session_state.sel_ticker_watch,
                                      st.session_state.sel_name_watch,
                                      rsi_snapshot, cb_overhang, surge_reasons)

    with tab3:
        st.markdown("<div class='section-title'>🔍 종목검색</div>", unsafe_allow_html=True)
        srch = st.text_input("검색어", placeholder="예: 삼성전자 / 005930",
                              label_visibility="collapsed", key="stock_search_input")
        krx = get_krx_listing()
        if not krx:
            st.info("종목 목록을 불러오는 중... P1이 krx_listing.json을 export하지 않았을 수 있습니다.")
        elif srch.strip():
            df_krx = pd.DataFrame(krx)
            q = srch.strip()
            if q.isdigit():
                result_krx = df_krx[df_krx["종목코드"].str.startswith(q.zfill(max(len(q), 4)))]
            else:
                result_krx = df_krx[df_krx["종목명"].str.contains(q, case=False, na=False)]
            result_krx = result_krx.head(30).reset_index(drop=True)
            if result_krx.empty:
                st.info("검색 결과 없음")
            else:
                sel_krx = st.dataframe(result_krx, use_container_width=True, hide_index=True,
                                       on_select="rerun", selection_mode="single-row")
                rows_krx = sel_krx.selection.rows if hasattr(sel_krx, "selection") else []
                if rows_krx:
                    r = result_krx.iloc[rows_krx[0]]
                    st.session_state.sel_ticker_surge = str(r["종목코드"]).zfill(6)
                    st.session_state.sel_name_surge   = r["종목명"]
                if st.session_state.get("sel_ticker_surge"):
                    render_detail(st.session_state.sel_ticker_surge,
                                  st.session_state.sel_name_surge,
                                  rsi_snapshot, cb_overhang, surge_reasons)



if __name__ == "__main__":
    main()
