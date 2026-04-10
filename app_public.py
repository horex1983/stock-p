"""
app_public.py — 급등주 모멘텀 대시보드 (공개 배포용)
합법 소스: 금융위원회 공공API + DART + yfinance + GitHub 공유데이터
"""

import os, json, base64, logging, requests
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import yfinance as yf
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

load_dotenv()

st.set_page_config(page_title="급등주 모멘텀 대시보드", page_icon="📈",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
[data-testid="stAppViewContainer"] { background: #0e1117; }
[data-testid="stSidebar"] { background: #161b22; }
.metric-card { background: #161b22; border: 1px solid #30363d;
    border-radius: 8px; padding: 12px 16px; margin-bottom: 8px; }
.metric-label { color: #8b949e; font-size: 0.78em; margin-bottom: 2px; }
.metric-value { color: #f0f6fc; font-size: 1.1em; font-weight: 700; }
.up   { color: #f85149 !important; }
.down { color: #58a6ff !important; }
.section-title { color: #f0f6fc; font-size: 1.05em; font-weight: 700;
    border-left: 3px solid #238636; padding-left: 8px; margin: 16px 0 8px 0; }
</style>""", unsafe_allow_html=True)


def _secret(key, default=""):
    try:
        if key in st.secrets: return st.secrets[key]
    except: pass
    v = os.getenv(key, default)
    return v.strip() if isinstance(v, str) else v


PUBLIC_DATA_API_KEY = _secret("PUBLIC_DATA_API_KEY")
DART_API_KEY        = _secret("DART_API_KEY")
GITHUB_TOKEN        = _secret("GITHUB_TOKEN")
GITHUB_REPO         = _secret("GITHUB_REPO")
GITHUB_BRANCH       = _secret("GITHUB_BRANCH", "main")
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


@st.cache_data(ttl=1800)
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


_PUB = "https://apis.data.go.kr/1160100/service"


def _pub(endpoint, params):
    if not PUBLIC_DATA_API_KEY: return None
    try:
        r = requests.get(f"{_PUB}/{endpoint}",
            params={"serviceKey": PUBLIC_DATA_API_KEY, "resultType": "json", **params},
            timeout=10)
        d = r.json()
        if d.get("response", {}).get("header", {}).get("resultCode") != "00": return None
        return d["response"]["body"]
    except: return None


def _latest_biz_date():
    d = date.today()
    if datetime.now().hour < 13: d -= timedelta(days=1)
    while d.weekday() >= 5: d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


@st.cache_data(ttl=3600)
def _get_all_stocks_raw():
    """Fetch all stocks for the latest business date (shared by surge ranking and watchlist)."""
    bas_dt = _latest_biz_date()
    body = _pub("GetStockSecuritiesInfoService/getStockPriceInfo",
                {"numOfRows": 3000, "pageNo": 1, "basDt": bas_dt})
    if not body: return pd.DataFrame()
    raw = body.get("items", {}).get("item", [])
    items = raw if isinstance(raw, list) else [raw]
    if not items: return pd.DataFrame()
    df = pd.DataFrame(items)
    for col in ["fltRt", "trqu", "clpr"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def get_surge_ranking(top_n=50):
    df = _get_all_stocks_raw()
    if df.empty: return pd.DataFrame()
    excl = ["ETF", "ETN", "SPAC", "인버스", "레버리지", "선물", "합성"]
    df = df[~df["itmsNm"].str.contains("|".join(excl), na=False)]
    df = df[(df["trqu"] >= 100_000) & (df["fltRt"] > 0)]
    df = df.sort_values("fltRt", ascending=False).head(top_n).reset_index(drop=True)
    df.insert(0, "순위", df.index + 1)
    return df


def get_watchlist_prices(codes):
    df = _get_all_stocks_raw()
    if df.empty or not codes: return pd.DataFrame()
    codes_padded = [str(c).zfill(6) for c in codes]
    return df[df["srtnCd"].isin(codes_padded)].copy().reset_index(drop=True)


@st.cache_data(ttl=3600)
def get_indices():
    bas_dt = _latest_biz_date()
    result = {}
    for nm in ("코스피", "코스닥", "코스피 200"):
        body = _pub("GetMarketIndexInfoService/getStockMarketIndex",
                    {"numOfRows": 1, "pageNo": 1, "basDt": bas_dt, "idxNm": nm})
        if not body: continue
        raw = body.get("items", {}).get("item", [])
        items = raw if isinstance(raw, list) else [raw]
        if items:
            result[nm] = items[0]
    return result


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
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.75, 0.25], vertical_spacing=0.02)
    fig.add_trace(go.Candlestick(
        x=df.index, open=df["Open"], high=df["High"], low=df["Low"], close=df["Close"],
        name="가격", increasing_line_color="#f85149", decreasing_line_color="#58a6ff",
        increasing_fillcolor="#f85149", decreasing_fillcolor="#58a6ff"), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["MA5"],  name="MA5",
                             line=dict(color="#3fb950", width=1.2, dash="dot")), row=1, col=1)
    fig.add_trace(go.Scatter(x=df.index, y=df["MA20"], name="MA20",
                             line=dict(color="#e3b341", width=1.5)), row=1, col=1)
    close_v = df["Close"].values.flatten()
    open_v  = df["Open"].values.flatten()
    colors  = ["#f85149" if c >= o else "#58a6ff" for c, o in zip(close_v, open_v)]
    fig.add_trace(go.Bar(x=df.index, y=df["Volume"].values.flatten(),
                         name="거래량", marker_color=colors, opacity=0.6), row=2, col=1)
    fig.update_layout(
        template="plotly_dark", height=420, margin=dict(l=0, r=0, t=30, b=0),
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


# --- AgGrid dark theme config ---

_AGGRID_CSS = {
    "#gridToolBar":          {"display": "none"},
    ".ag-root-wrapper":      {"background": "#161b22 !important",
                              "border": "1px solid #30363d !important",
                              "border-radius": "8px"},
    ".ag-header":            {"background": "#0d1117 !important",
                              "border-bottom": "1px solid #30363d !important"},
    ".ag-header-cell":       {"background": "#0d1117 !important"},
    ".ag-header-cell-label": {"color": "#8b949e !important", "font-size": "0.82em"},
    ".ag-row":               {"background": "#161b22 !important",
                              "border-color": "#21262d !important"},
    ".ag-row-hover":         {"background": "#1c2333 !important"},
    ".ag-row-selected":      {"background": "#1f3a5f !important"},
    ".ag-cell":              {"color": "#f0f6fc !important"},
    ".ag-body-viewport":     {"background": "#161b22 !important"},
    ".ag-paging-panel":      {"background": "#0d1117 !important",
                              "color": "#8b949e !important",
                              "border-top": "1px solid #30363d !important"},
}

_CELL_FLTRT = JsCode("""
function(params) {
    if (params.value > 0) return {color: '#f85149', fontWeight: 'bold'};
    if (params.value < 0) return {color: '#58a6ff', fontWeight: 'bold'};
    return {color: '#8b949e'};
}
""")


def _build_aggrid(disp_df, height=520, has_rank=True):
    gb = GridOptionsBuilder.from_dataframe(disp_df)
    gb.configure_default_column(
        resizable=True, sortable=True, filter=False,
        cellStyle={"color": "#f0f6fc", "backgroundColor": "transparent"})

    col_cfg = {
        "순위":       dict(width=60,  pinned="left"),
        "종목코드":   dict(width=90),
        "종목명":     dict(width=120),
        "시장":       dict(width=72),
        "종가":       dict(width=92,  type=["numericColumn"],
                          valueFormatter="x.toLocaleString()"),
        "등락률(%)":  dict(width=90,  type=["numericColumn"],
                          valueFormatter="x.toFixed(2)+'%'",
                          cellStyle=_CELL_FLTRT),
        "거래량":     dict(width=112, type=["numericColumn"],
                          valueFormatter="x.toLocaleString()"),
        "기준일":     dict(width=90),
        "급등이유":   dict(flex=1),
    }
    for col, kwargs in col_cfg.items():
        if col in disp_df.columns:
            gb.configure_column(col, **kwargs)

    gb.configure_selection(selection_mode="single", use_checkbox=False)
    gb.configure_grid_options(rowHeight=32, headerHeight=36,
                               suppressMovableColumns=True,
                               pagination=True, paginationPageSize=25)
    return AgGrid(
        disp_df,
        gridOptions=gb.build(),
        theme="alpine",
        allow_unsafe_jscode=True,
        use_container_width=True,
        height=height,
        custom_css=_AGGRID_CSS,
    )


def _get_selected_row(resp):
    """Safely extract the first selected row as a dict from an AgGrid response."""
    sel = resp.get("selected_rows", None)
    if sel is None:
        return None
    if hasattr(sel, "iloc"):        # pandas DataFrame (newer streamlit-aggrid)
        return sel.iloc[0].to_dict() if len(sel) > 0 else None
    if isinstance(sel, list):       # list of dicts (older streamlit-aggrid)
        return sel[0] if len(sel) > 0 else None
    return None


def render_sidebar(indices):
    with st.sidebar:
        st.markdown("### 📊 시장 지수 (T+1)")
        for nm, it in indices.items():
            try:
                clpr  = float(it.get("clpr", 0))
                fltRt = float(it.get("fltRt", 0))
                sign  = "▲" if fltRt >= 0 else "▼"
                cls   = "up" if fltRt >= 0 else "down"
                st.markdown(
                    f"<div class='metric-card'><div class='metric-label'>{nm}</div>"
                    f"<div class='metric-value'>{clpr:,.2f} "
                    f"<span class='{cls}'>{sign} {abs(fltRt):.2f}%</span></div></div>",
                    unsafe_allow_html=True)
            except: pass
        st.divider()
        meta = get_meta()
        if meta: st.caption(f"🔄 수집: {meta.get('last_exported_at', '-')}")
        st.caption("⚠️ 전일 종가 기준 (T+1)")
        st.caption("📈 차트: yfinance (15분 지연)")


def render_table(df, surge_reasons):
    if df.empty:
        st.warning("공공데이터 API 응답 없음.")
        return None
    disp = df[["순위", "srtnCd", "itmsNm", "mrktCtg", "clpr", "fltRt", "trqu", "basDt"]].copy()
    disp["급등이유"] = disp["srtnCd"].apply(
        lambda t: surge_reasons.get(str(t).zfill(6), {}).get("reason", "")
        if isinstance(surge_reasons.get(str(t).zfill(6), {}), dict) else "")
    disp = disp.rename(columns={"srtnCd": "종목코드", "itmsNm": "종목명", "mrktCtg": "시장",
        "clpr": "종가", "fltRt": "등락률(%)", "trqu": "거래량", "basDt": "기준일"})
    return _build_aggrid(disp), df


def render_detail(ticker, name, rsi_snapshot, cb_overhang):
    st.divider()
    st.markdown(f"<div class='section-title'>📋 {name} ({ticker})</div>", unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    for col, label, key in [(c1, "RSI 일봉", "_daily"), (c2, "RSI 주봉", "_1wk"), (c3, "RSI 5분봉", "_5m")]:
        data = rsi_snapshot.get(f"{ticker}{key}", {})
        with col:
            v   = data.get("rsi", "-")
            sig = data.get("signal", "")
            st.markdown(
                f"<div class='metric-card'><div class='metric-label'>{label}</div>"
                f"<div class='metric-value'>{v}</div>"
                f"<div style='font-size:0.8em;color:#8b949e'>{sig}</div></div>",
                unsafe_allow_html=True)
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
                 "제목": it.get("report_nm", ""), "제출인": it.get("flr_nm", "")} for it in items]
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.caption("최근 공시 없음")


def render_news(news_items):
    if not news_items:
        st.info("테마뉴스 없음. Project 1 데몬이 실행 중인지 확인하세요.")
        return
    for item in news_items[:30]:
        title = item.get("title", "")
        body  = item.get("text") or item.get("content") or item.get("body", "")
        dt    = str(item.get("sendDate") or item.get("date", ""))[:10]
        with st.expander(f"[{dt}] {title}"):
            if body:
                st.markdown(str(body)[:600] + ("..." if len(str(body)) > 600 else ""))


def main():
    st.title("📈 급등주 모멘텀 대시보드 (공개)")
    st.caption("금융위원회 공공API · DART · yfinance · GitHub 공유데이터")

    for key, val in [("sel_ticker_surge", ""), ("sel_name_surge", ""),
                     ("sel_ticker_watch", ""), ("sel_name_watch", "")]:
        if key not in st.session_state:
            st.session_state[key] = val

    indices       = get_indices()
    surge_df      = get_surge_ranking()
    surge_reasons = get_surge_reasons()
    rsi_snapshot  = get_rsi_snapshot()
    cb_overhang   = get_cb_overhang()
    theme_news    = get_theme_news()
    watchlist     = get_watchlist()

    render_sidebar(indices)

    tab1, tab2, tab3 = st.tabs(["🚀 급등주 랭킹", "⭐ 관심종목", "📰 테마뉴스"])

    with tab1:
        st.markdown("<div class='section-title'>🚀 급등주 랭킹 - 전일 종가 기준 (T+1)</div>",
                    unsafe_allow_html=True)
        result = render_table(surge_df, surge_reasons)
        if result:
            resp, df = result
            row_data = _get_selected_row(resp)
            if row_data:
                ticker = str(row_data.get("종목코드", "")).zfill(6)
                name   = row_data.get("종목명", "")
                if ticker:
                    st.session_state.sel_ticker_surge = ticker
                    st.session_state.sel_name_surge   = name
            if st.session_state.sel_ticker_surge:
                render_detail(st.session_state.sel_ticker_surge,
                              st.session_state.sel_name_surge,
                              rsi_snapshot, cb_overhang)

    with tab2:
        st.markdown("<div class='section-title'>⭐ 관심종목 - 전일 종가 기준 (T+1)</div>",
                    unsafe_allow_html=True)
        if not watchlist:
            st.info("관심종목 없음. Project 1에서 관심종목을 등록하세요.")
        else:
            wdf = get_watchlist_prices(watchlist)
            if wdf.empty:
                st.warning("관심종목 시세 조회 실패.")
            else:
                disp = wdf[["srtnCd", "itmsNm", "mrktCtg", "clpr", "fltRt", "trqu", "basDt"]].copy()
                disp = disp.rename(columns={"srtnCd": "종목코드", "itmsNm": "종목명",
                    "mrktCtg": "시장", "clpr": "종가", "fltRt": "등락률(%)",
                    "trqu": "거래량", "basDt": "기준일"})
                resp_w = _build_aggrid(disp, height=400, has_rank=False)
                row_data_w = _get_selected_row(resp_w)
                if row_data_w:
                    ticker = str(row_data_w.get("종목코드", "")).zfill(6)
                    name   = row_data_w.get("종목명", "")
                    if ticker:
                        st.session_state.sel_ticker_watch = ticker
                        st.session_state.sel_name_watch   = name
                if st.session_state.sel_ticker_watch:
                    render_detail(st.session_state.sel_ticker_watch,
                                  st.session_state.sel_name_watch,
                                  rsi_snapshot, cb_overhang)

    with tab3:
        st.markdown("<div class='section-title'>📰 테마뉴스 (infostock 수집)</div>",
                    unsafe_allow_html=True)
        render_news(theme_news)


if __name__ == "__main__":
    main()
