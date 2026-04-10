"""
app_public.py — 급등주 모멘텀 대시보드 (공개 배포용)
합법 소스: 금융위원회 공공API + DART + yfinance + GitHub 공유데이터
"""

import os, json, base64, logging, requests, re
from collections import Counter
import streamlit as st
import streamlit.components.v1 as _components
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf
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
</style>""", unsafe_allow_html=True)


def _secret(key, default=""):
    try:
        if key in st.secrets: return st.secrets[key]
    except: pass
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


DART_API_KEY = _secret("DART_API_KEY")
GITHUB_TOKEN        = _secret("GITHUB_TOKEN")
GITHUB_REPO         = _secret("GITHUB_REPO")
GITHUB_BRANCH       = _secret("GITHUB_BRANCH", "main")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@st.cache_data(ttl=300)
def _github_json(path):
    if not GITHUB_TOKEN or not GITHUB_REPO: return None
    try:
        r = requests.get(
            f"https://api.github.com/repos/{GITHUB_REPO}/contents/{path}",
            headers={"Authorization": f"Bearer {GITHUB_TOKEN}",
                     "Accept": "application/vnd.github+json"},
            params={"ref": GITHUB_BRANCH}, timeout=10)
        if r.status_code != 200: return None
        return json.loads(base64.b64decode(r.json()["content"]).decode())
    except: return None


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

def get_indices_p1():
    """P1 exported index data: KOSPI/KOSDAQ/NASDAQ/S&P500 + disparity."""
    d = _github_json("data/indices.json")
    return d if isinstance(d, dict) else {}

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


@st.cache_data(ttl=900)
def get_ohlcv(ticker):
    for suffix in [".KS", ".KQ"]:
        try:
            df = yf.download(ticker + suffix, period="6mo", progress=False, auto_adjust=True)
            if not df.empty: return df
        except: pass
    return pd.DataFrame()


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
        name="가격", increasing_line_color="#f85149", decreasing_line_color="#58a6ff",
        increasing_fillcolor="#f85149", decreasing_fillcolor="#58a6ff"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["MA5"],  name="MA5",
                             line=dict(color="#3fb950", width=1.2, dash="dot")), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["MA20"], name="MA20",
                             line=dict(color="#e3b341", width=1.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["MA60"], name="MA60",
                             line=dict(color="#a371f7", width=1.5)), row=1, col=1)
    # Volume
    close_v = df["Close"].values.flatten()
    open_v  = df["Open"].values.flatten()
    colors  = ["#f85149" if c >= o else "#58a6ff" for c, o in zip(close_v, open_v)]
    vol_ma  = pd.Series(df["Volume"].values.flatten()).rolling(20).mean()
    fig.add_trace(go.Bar(x=df.index, y=df["Volume"].values.flatten(),
                         name="거래량", marker_color=colors, opacity=0.5), row=2, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=vol_ma,
                             name="거래량MA20", line=dict(color="#e3b341", width=1)),
                  row=2, col=1)
    fig.update_layout(
        template="plotly_dark", height=460, margin=dict(l=0, r=0, t=30, b=0),
        title=dict(text=f"{name} ({ticker}) - 6개월 (15분 지연)", font=dict(size=13), x=0),
        xaxis_rangeslider_visible=False, legend=dict(orientation="h", y=1.08, x=0),
        paper_bgcolor="#0e1117", plot_bgcolor="#0e1117")
    fig.update_yaxes(gridcolor="#21262d")
    fig.update_xaxes(gridcolor="#21262d")
    st.plotly_chart(fig, use_container_width=True)


@st.cache_data(ttl=1800)
def get_dart(ticker):
    if not DART_API_KEY: return []
    try:
        end = datetime.now()
        r = requests.get("https://opendart.fss.or.kr/api/list.json", params={
            "crtfc_key": DART_API_KEY, "stock_code": ticker,
            "bgn_de": (end - timedelta(days=60)).strftime("%Y%m%d"),
            "end_de": end.strftime("%Y%m%d"), "page_count": 20}, timeout=10)
        d = r.json()
        return d.get("list", []) if d.get("status") == "000" else []
    except: return []


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


def render_sidebar(indices_pub, indices_p1, surge_items, theme_items):
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
        else:
            st.caption("지수 데이터 없음 (P1 미수집)")

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

        st.divider()

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
                        if shown == 0:
                            st.caption("표시할 항목 없음")
                    else:
                        st.caption("수집된 데이터 없음")

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
                        if not entries:
                            st.caption("수집된 데이터 없음")
                    else:
                        st.caption("수집된 데이터 없음")
            st.divider()

        # ── 키워드 워드클라우드 ───────────────────────────────────────────────
        top_kw = _get_top_keywords(surge_items, n=8)
        if top_kw:
            st.markdown("### 🏷️ 핫 키워드", unsafe_allow_html=True)
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
                        f"<div style='padding:6px 2px;border-bottom:1px solid #30363d;'>"
                        f"<span style='font-size:1.0em;font-weight:700;'>{_medals[_i]} {_kw}</span>"
                        f"<div style='background:#21262d;border-radius:4px;height:6px;margin-top:4px;'>"
                        f"<div style='background:#1565C0;width:{_bar}%;height:6px;border-radius:4px;'></div>"
                        f"</div></div>", unsafe_allow_html=True)
                if not _top5:
                    st.caption("데이터 없음")
            st.divider()

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
        st.divider()

        # ── 필터 ─────────────────────────────────────────────────────────────
        st.markdown(
            "<div style='margin-bottom:6px;padding-bottom:5px;border-bottom:1px solid #E8ECF4;'>"
            "<strong>🔍 필터</strong></div>", unsafe_allow_html=True)
        market_filter = st.radio("시장 구분", ["전체", "KOSPI", "KOSDAQ"],
                                  horizontal=True, label_visibility="collapsed")

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
            f"장: {_mkt_label} &nbsp;·&nbsp; 차트: yfinance 15분 지연"
            f"</div>",
            unsafe_allow_html=True)

        if st.button("🔄 데이터 새로고침", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    return market_filter


def render_p1_table(surge_table, rsi_snapshot, watchlist=None, market_filter="전체"):
    """Display P1 exported surge table (tier/theme/risk pre-computed by P1)."""
    df = pd.DataFrame(surge_table)
    if df.empty:
        return None
    df.insert(0, "순위", range(1, len(df) + 1))
    # RSI signals from rsi_snapshot.json
    def _sig(t, suffix):
        d = rsi_snapshot.get(f"{t}{suffix}", {})
        return d.get("signal", "") if isinstance(d, dict) else ""
    df["신호"]    = df["종목코드"].apply(lambda t: _sig(t, "_daily"))
    df["단기신호"] = df["종목코드"].apply(lambda t: _sig(t, "_5m"))
    df["장기신호"] = df["종목코드"].apply(lambda t: _sig(t, "_weekly"))

    # ★ watchlist marker — sort watchlist items first
    _wl = set(str(c).zfill(6) for c in (watchlist or []))
    df.insert(0, "★", df["종목코드"].apply(lambda c: "★" if str(c).zfill(6) in _wl else ""))
    df = df.sort_values("★", ascending=False, kind="stable")

    if market_filter != "전체":
        df = df[df["시장"] == market_filter]

    search = st.text_input("🔍 종목 검색", placeholder="종목명 또는 코드 입력",
                            label_visibility="collapsed")
    if search:
        mask = (df["종목명"].str.contains(search, na=False) |
                df["종목코드"].str.contains(search, na=False))
        df = df[mask]

    disp_cols = ["★", "순위", "종목코드", "종목명", "시장", "현재가", "당일등락",
                 "7일누적", "거래대금", "tier", "테마", "신호", "단기신호", "장기신호", "리스크"]
    disp = df[[c for c in disp_cols if c in df.columns]].copy()
    col_cfg = {
        "★":      st.column_config.TextColumn(width="small"),
        "순위":    st.column_config.NumberColumn(width="small"),
        "tier":   st.column_config.TextColumn("티어", width="small"),
        "현재가":  st.column_config.NumberColumn(format="%d", width="small"),
        "당일등락": st.column_config.NumberColumn(label="당일등락(%)", format="%.2f%%", width="small"),
        "7일누적": st.column_config.NumberColumn(label="7일누적(%)", format="%.2f%%", width="small"),
        "거래대금": st.column_config.NumberColumn(label="거래대금(억)", format="%.1f", width="small"),
        "테마":    st.column_config.TextColumn(width="medium"),
        "신호":    st.column_config.TextColumn(width="small"),
        "단기신호": st.column_config.TextColumn("단기신호", width="small"),
        "장기신호": st.column_config.TextColumn("장기신호", width="small"),
        "리스크":  st.column_config.TextColumn(width="small"),
    }
    selected = st.dataframe(disp, use_container_width=True, hide_index=True,
                            on_select="rerun", selection_mode="single-row",
                            column_config=col_cfg)
    return selected, df


def render_table(df, surge_reasons, market_filter="전체"):
    """Fallback table using public API data (when P1 surge_table unavailable)."""
    if df.empty:
        st.warning("공공데이터 API 응답 없음.")
        return None
    filtered = df.copy()
    if market_filter != "전체":
        filtered = filtered[filtered["mrktCtg"].str.upper() == market_filter.upper()]
    # Search box
    search = st.text_input("🔍 종목 검색", placeholder="종목명 또는 코드 입력",
                            label_visibility="collapsed")
    if search:
        mask = (filtered["itmsNm"].str.contains(search, na=False) |
                filtered["srtnCd"].str.contains(search, na=False))
        filtered = filtered[mask]
    disp = _make_display_df(filtered, surge_reasons)
    col_cfg = {
        "순위":      st.column_config.NumberColumn(width="small"),
        "티어":      st.column_config.TextColumn(width="small"),
        "등락률(%)": st.column_config.NumberColumn(format="%.2f%%", width="small"),
        "거래량":    st.column_config.NumberColumn(format="%d"),
        "급등이유":  st.column_config.TextColumn(width="large"),
    }
    selected = st.dataframe(disp, use_container_width=True, hide_index=True,
                            on_select="rerun", selection_mode="single-row",
                            column_config=col_cfg)
    return selected, filtered


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


def _rsi_gauge(label, rsi_val, signal):
    """Render a compact Plotly RSI gauge chart."""
    try:
        v = float(rsi_val)
    except (TypeError, ValueError):
        v = None

    if v is None:
        bar_color = "#30363d"
        needle_v  = 50
    elif v >= 70:
        bar_color = "#f85149"   # overbought — red
        needle_v  = v
    elif v <= 30:
        bar_color = "#58a6ff"   # oversold — blue
        needle_v  = v
    else:
        bar_color = "#3fb950"   # neutral — green
        needle_v  = v

    display = f"{v:.1f}" if v is not None else "-"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=needle_v if v is not None else 50,
        number={"suffix": "", "font": {"size": 22, "color": bar_color}},
        title={"text": f"<b>{label}</b><br><span style='font-size:0.75em;color:#8b949e'>{signal}</span>",
               "font": {"size": 12, "color": "#8b949e"}},
        gauge={
            "axis": {"range": [0, 100], "tickwidth": 1, "tickcolor": "#30363d",
                     "tickvals": [0, 30, 50, 70, 100],
                     "ticktext": ["0", "30", "50", "70", "100"],
                     "tickfont": {"size": 9, "color": "#8b949e"}},
            "bar":  {"color": bar_color, "thickness": 0.25},
            "bgcolor": "#161b22",
            "borderwidth": 0,
            "steps": [
                {"range": [0,  30], "color": "#0d2137"},
                {"range": [30, 70], "color": "#1a1f2e"},
                {"range": [70, 100], "color": "#2d0d0d"},
            ],
            "threshold": {
                "line": {"color": "#f0f6fc", "width": 2},
                "thickness": 0.75,
                "value": needle_v if v is not None else 50,
            },
        },
    ))
    fig.update_layout(
        height=160, margin=dict(l=10, r=10, t=40, b=5),
        paper_bgcolor="#161b22", plot_bgcolor="#161b22",
        font={"color": "#f0f6fc"},
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_detail(ticker, name, rsi_snapshot, cb_overhang, surge_reasons=None):
    st.divider()
    st.markdown(f"<div class='section-title'>📋 {name} ({ticker})</div>", unsafe_allow_html=True)

    # Surge reason banner
    if surge_reasons:
        reason_data = surge_reasons.get(str(ticker).zfill(6), {})
        reason_txt  = reason_data.get("reason", "") if isinstance(reason_data, dict) else ""
        if reason_txt:
            st.info(f"📌 급등이유: {reason_txt}")

    # Row 1: extra daily stats + 6개월 고저  (P1 corp_info.json)
    corp  = get_corp_info(ticker)
    extra = {k: v for k, v in corp.items()
             if k in ("시가", "고가", "저가", "거래대금", "시가총액") and v}
    ohlcv = get_ohlcv(ticker)
    if not ohlcv.empty:
        hi_6m = float(ohlcv["High"].max())
        lo_6m = float(ohlcv["Low"].min())
        cur   = float(ohlcv["Close"].iloc[-1])
        pos   = (cur - lo_6m) / (hi_6m - lo_6m) * 100 if hi_6m != lo_6m else 0
        extra["6개월 고가"] = int(hi_6m)
        extra["6개월 저가"] = int(lo_6m)
        extra["고저위치"]  = f"{pos:.0f}%"

    if extra:
        ec = st.columns(len(extra))
        for i, (lbl, val) in enumerate(extra.items()):
            with ec[i]:
                fmt = f"{val:,}" if isinstance(val, int) else str(val)
                color = ""
                if lbl == "고저위치":
                    pct = float(str(val).replace("%", ""))
                    color = ("color:#d32f2f" if pct >= 70
                             else "color:#1565c0" if pct <= 30
                             else "color:#2e7d32")
                st.markdown(
                    f"<div class='metric-card'><div class='metric-label'>{lbl}</div>"
                    f"<div class='metric-value' style='font-size:0.95em;{color}'>{fmt}</div></div>",
                    unsafe_allow_html=True)

    # Row 2: company outline + financial summary (P1 corp_info.json)
    outline = corp.get("outline", {})
    fin     = corp.get("financial", {})

    if outline or fin:
        with st.expander("📊 기업정보 · 재무요약", expanded=False):
            if outline:
                oc = st.columns(4)
                outline_fields = [
                    ("sicNm",       "업종"),
                    ("enpEstbDt",   "설립일"),
                    ("enpEmpeCnt",  "직원수"),
                    ("enpStacMm",   "결산월"),
                ]
                for i, (field, label) in enumerate(outline_fields):
                    val = outline.get(field, "-") or "-"
                    with oc[i]:
                        st.markdown(
                            f"<div class='metric-card'><div class='metric-label'>{label}</div>"
                            f"<div class='metric-value' style='font-size:0.9em'>{val}</div></div>",
                            unsafe_allow_html=True)
            if fin:
                biz_year = fin.get("bizYear", "")
                st.caption(f"재무 기준: {biz_year}년 ({fin.get('fnclDcdNm', '연결/개별 혼합')})")
                fc = st.columns(4)
                fin_fields = [
                    ("enpSaleAmt",  "매출액"),
                    ("enpBzopPft",  "영업이익"),
                    ("enpCrtmNpf",  "당기순이익"),
                    ("fnclDebtRto", "부채비율(%)"),
                ]
                for i, (field, label) in enumerate(fin_fields):
                    raw_val = fin.get(field, "-")
                    try:
                        v = float(raw_val)
                        fmt = f"{v:,.0f}" if label != "부채비율(%)" else f"{v:.1f}%"
                    except (TypeError, ValueError):
                        fmt = str(raw_val) if raw_val else "-"
                    with fc[i]:
                        st.markdown(
                            f"<div class='metric-card'><div class='metric-label'>{label}</div>"
                            f"<div class='metric-value' style='font-size:0.9em'>{fmt}</div></div>",
                            unsafe_allow_html=True)

    # Row 3: RSI gauges + overhang
    c1, c2, c3, c4 = st.columns(4)
    for col, label, key in [(c1, "RSI 일봉", "_daily"), (c2, "RSI 주봉", "_1wk"), (c3, "RSI 5분봉", "_5m")]:
        data = rsi_snapshot.get(f"{ticker}{key}", {})
        with col:
            _rsi_gauge(label, data.get("rsi"), data.get("signal", ""))
    with c4:
        has   = bool(cb_overhang.get(ticker))
        lbl   = "⚠️ 오버행 있음" if has else "✅ 오버행 없음"
        color = "#f85149" if has else "#3fb950"
        st.markdown(
            f"<div class='metric-card'><div class='metric-label'>CB/BW 오버행</div>"
            f"<div class='metric-value' style='color:{color}'>{lbl}</div></div>",
            unsafe_allow_html=True)
    render_chart(ticker, name)
    st.markdown("<div class='section-title'>📋 DART 공시 (최근 60일)</div>", unsafe_allow_html=True)
    _TYPE = {"A": "정기공시", "B": "주요사항", "C": "발행공시", "D": "지분공시",
             "E": "기타", "F": "외부감사", "I": "거래소공시"}
    items = get_dart(ticker)
    if items:
        rows = [{"날짜": it.get("rcept_dt", "")[:8],
                 "구분": _TYPE.get(it.get("pblntf_ty", ""), it.get("pblntf_ty", "")),
                 "제목": it.get("report_nm", ""),
                 "제출인": it.get("flr_nm", ""),
                 "링크": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={it.get('rcept_no', '')}"
                         if it.get("rcept_no") else ""} for it in items]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True,
                     column_config={
                         "링크": st.column_config.LinkColumn("링크", width="small",
                                                              display_text="열기")})
    else:
        st.caption("최근 공시 없음")


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
    st.caption("DART · yfinance · GitHub 공유데이터 (P1 실시간)")

    for key, val in [("sel_ticker_surge", ""), ("sel_name_surge", ""),
                     ("sel_ticker_watch", ""), ("sel_name_watch", "")]:
        if key not in st.session_state:
            st.session_state[key] = val

    indices_p1    = get_indices_p1()
    surge_table   = get_surge_table()
    surge_reasons = get_surge_reasons()
    rsi_snapshot  = get_rsi_snapshot()
    cb_overhang   = get_cb_overhang()
    watchlist     = get_watchlist()
    surge_items, theme_items = get_accumulated_news()
    theme_news    = surge_items + theme_items  # all items for 테마뉴스 탭

    market_filter = render_sidebar({}, indices_p1, surge_items, theme_items)

    tab1, tab2, tab3, tab4 = st.tabs(["🚀 급등주 랭킹", "⭐ 관심종목", "🔍 종목검색", "📰 테마뉴스"])

    with tab1:
        if surge_table:
            st.markdown("<div class='section-title'>🚀 급등주 랭킹 — P1 실시간 데이터</div>",
                        unsafe_allow_html=True)
            result = render_p1_table(surge_table, rsi_snapshot, watchlist, market_filter)
            if result:
                selected, df_p1 = result
                rows = selected.selection.rows if hasattr(selected, "selection") else []
                if rows:
                    row = df_p1.iloc[rows[0]]
                    st.session_state.sel_ticker_surge = str(row["종목코드"]).zfill(6)
                    st.session_state.sel_name_surge   = row["종목명"]
                if st.session_state.sel_ticker_surge:
                    render_detail(st.session_state.sel_ticker_surge,
                                  st.session_state.sel_name_surge,
                                  rsi_snapshot, cb_overhang, surge_reasons)
        else:
            st.info("P1 데이터 없음. Project 1 데몬이 실행 중인지 확인하세요.")

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

    with tab4:
        st.markdown("<div class='section-title'>📰 테마뉴스 (infostock 수집)</div>",
                    unsafe_allow_html=True)
        render_news(theme_news)


if __name__ == "__main__":
    main()
