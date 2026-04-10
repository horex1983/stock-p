"""
app_public.py — KIS Premium Momentum Dashboard (Project 2 / Streamlit Cloud 공개 배포)

합법 소스만 사용:
  - 금융위원회 공공API (apis.data.go.kr) → 시세, 급등랭킹, 지수
  - DART API (opendart.fss.or.kr)         → 공시/감사의견
  - yfinance                               → 현재가 보완 (15분 지연)
  - GitHub private repo (JSON)             → Project 1 수집 데이터
      · accumulated.json   (infostock 테마뉴스)
      · surge_reasons_db.json (급등이유)
      · rsi_snapshot.json  (RSI 스냅샷)
      · cb_overhang_cache.json (CB/BW 오버행)

실행:
    streamlit run app_public.py

환경변수 (Streamlit Secrets 또는 .env):
    PUBLIC_DATA_API_KEY   data.go.kr API 키
    DART_API_KEY          opendart.fss.or.kr API 키
    GITHUB_TOKEN          GitHub Personal Access Token (repo:read 권한)
    GITHUB_REPO           데이터 저장소 (예: yourname/my-stock-data)
    GITHUB_BRANCH         브랜치 (기본값: main)
"""

import os
import json
import base64
import logging
import requests
import streamlit as st
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta, date
from dotenv import load_dotenv

load_dotenv()

# ── 페이지 설정 ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="급등주 모멘텀 대시보드 (공개)",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Config ────────────────────────────────────────────────────────────────────

def _secret(key: str, default: str = "") -> str:
    """Streamlit Secrets → 환경변수 순으로 읽기."""
    try:
        if key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    val = os.getenv(key, default)
    return val.strip() if isinstance(val, str) else val


PUBLIC_DATA_API_KEY = _secret("PUBLIC_DATA_API_KEY")
DART_API_KEY        = _secret("DART_API_KEY")
GITHUB_TOKEN        = _secret("GITHUB_TOKEN")
GITHUB_REPO         = _secret("GITHUB_REPO")
GITHUB_BRANCH       = _secret("GITHUB_BRANCH", "main")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# ── GitHub JSON 읽기 ──────────────────────────────────────────────────────────

@st.cache_data(ttl=1800)   # 30분 캐시
def fetch_github_json(repo_path: str) -> dict | list | None:
    """
    GitHub API로 private repo 파일 읽기.
    repo_path: "data/accumulated.json" 형식
    """
    if not GITHUB_TOKEN or not GITHUB_REPO:
        return None
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{repo_path}"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        r = requests.get(url, headers=headers,
                         params={"ref": GITHUB_BRANCH}, timeout=10)
        if r.status_code != 200:
            log.warning(f"[github] {repo_path} → {r.status_code}")
            return None
        content = r.json().get("content", "")
        raw = base64.b64decode(content).decode("utf-8")
        return json.loads(raw)
    except Exception as e:
        log.error(f"[github] fetch failed {repo_path}: {e}")
        return None


def _get_meta() -> dict:
    meta = fetch_github_json("data/_meta.json") or {}
    return meta


# ── 공공데이터 API: 급등주 랭킹 ──────────────────────────────────────────────

_PUB_BASE = "https://apis.data.go.kr/1160100/service"

def _pub_get(endpoint: str, params: dict) -> dict | None:
    if not PUBLIC_DATA_API_KEY:
        return None
    merged = {"serviceKey": PUBLIC_DATA_API_KEY, "resultType": "json", **params}
    try:
        r = requests.get(f"{_PUB_BASE}/{endpoint}", params=merged, timeout=10)
        r.raise_for_status()
        data = r.json()
        header = data.get("response", {}).get("header", {})
        if header.get("resultCode") != "00":
            return None
        return data["response"]["body"]
    except Exception as e:
        log.error(f"[public] {endpoint}: {e}")
        return None


def _latest_business_date() -> str:
    d = date.today()
    if datetime.now().hour < 13:
        d -= timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d.strftime("%Y%m%d")


@st.cache_data(ttl=3600)
def get_surge_ranking(top_n: int = 50) -> pd.DataFrame:
    """
    공공API에서 등락률 상위 종목 조회 (T+1 기준).
    실시간 데이터가 아닌 전일 종가 기준임을 주의.
    """
    bas_dt = _latest_business_date()
    body = _pub_get(
        "GetStockSecuritiesInfoService/getStockPriceInfo",
        {"numOfRows": 3000, "pageNo": 1, "basDt": bas_dt},
    )
    if not body:
        return pd.DataFrame()

    raw = body.get("items", {}).get("item", [])
    items = raw if isinstance(raw, list) else [raw]
    if not items:
        return pd.DataFrame()

    df = pd.DataFrame(items)
    # 등락률 숫자 변환
    df["fltRt"] = pd.to_numeric(df.get("fltRt", 0), errors="coerce").fillna(0)
    df["trqu"]  = pd.to_numeric(df.get("trqu",  0), errors="coerce").fillna(0)
    df["clpr"]  = pd.to_numeric(df.get("clpr",  0), errors="coerce").fillna(0)

    # ETF/ETN/인버스/레버리지 제거 (종목명 패턴 필터)
    _EXCLUDE = ["ETF", "ETN", "SPAC", "인버스", "레버리지", "선물", "합성"]
    mask = ~df["itmsNm"].str.contains("|".join(_EXCLUDE), na=False)
    df = df[mask]

    # 거래량 10만 이상, 등락률 양수
    df = df[(df["trqu"] >= 100_000) & (df["fltRt"] > 0)]

    df = df.sort_values("fltRt", ascending=False).head(top_n).reset_index(drop=True)
    df["rank"] = df.index + 1
    return df[["rank", "srtnCd", "itmsNm", "mrktCtg", "clpr", "fltRt", "trqu", "basDt"]]


# ── 공공데이터 API: 지수 ──────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_market_indices() -> dict:
    bas_dt = _latest_business_date()
    body = _pub_get(
        "GetMarketIndexInfoService/getStockMarketIndex",
        {"numOfRows": 50, "pageNo": 1, "basDt": bas_dt},
    )
    if not body:
        return {}
    raw = body.get("items", {}).get("item", [])
    items = raw if isinstance(raw, list) else [raw]
    result = {}
    for it in items:
        nm = str(it.get("idxNm", ""))
        if nm in ("코스피", "코스닥", "코스피 200"):
            result[nm] = it
    return result


# ── 주가 차트 (yfinance) ──────────────────────────────────────────────────────

@st.cache_data(ttl=900)   # 15분
def get_price_history(ticker: str, period: str = "6mo") -> pd.DataFrame:
    """yfinance로 OHLCV 조회 (15분 지연)."""
    try:
        yf_ticker = f"{ticker}.KS"
        df = yf.download(yf_ticker, period=period, progress=False, auto_adjust=True)
        if df.empty:
            df = yf.download(f"{ticker}.KQ", period=period, progress=False, auto_adjust=True)
        return df
    except Exception as e:
        log.error(f"[yfinance] {ticker}: {e}")
        return pd.DataFrame()


# ── GitHub JSON: infostock 테마뉴스 ──────────────────────────────────────────

def get_theme_news() -> list:
    data = fetch_github_json("data/accumulated.json")
    if isinstance(data, dict):
        return data.get("items", [])
    return []


def get_surge_reasons() -> dict:
    data = fetch_github_json("data/surge_reasons_db.json")
    return data if isinstance(data, dict) else {}


def get_rsi_snapshot() -> dict:
    data = fetch_github_json("data/rsi_snapshot.json")
    return data if isinstance(data, dict) else {}


def get_cb_overhang() -> dict:
    data = fetch_github_json("data/cb_overhang_cache.json")
    return data if isinstance(data, dict) else {}


# ── UI: 사이드바 ──────────────────────────────────────────────────────────────

def render_sidebar(indices: dict):
    st.sidebar.title("📊 시장 지수 (T+1)")
    for nm, it in indices.items():
        clpr  = float(it.get("clpr", 0))
        fltRt = float(it.get("fltRt", 0))
        color = "🟢" if fltRt >= 0 else "🔴"
        st.sidebar.markdown(f"**{nm}** {color} `{clpr:,.2f}` ({fltRt:+.2f}%)")

    st.sidebar.divider()

    # 마지막 업데이트 시각
    meta = _get_meta()
    if meta:
        st.sidebar.caption(f"🔄 데이터 업데이트: {meta.get('last_exported_at', '-')}")
    st.sidebar.caption("⚠️ 시세는 전일 종가 기준 (T+1)")


# ── UI: 메인 테이블 ───────────────────────────────────────────────────────────

def render_surge_table(df: pd.DataFrame, surge_reasons: dict):
    st.subheader("🚀 급등주 랭킹 (전일 종가 기준, T+1)")

    if df.empty:
        st.warning("공공데이터 API 키가 없거나 데이터 수신 실패.")
        return

    # 급등 이유 컬럼 추가
    def _reason(row) -> str:
        ticker = str(row["srtnCd"]).zfill(6)
        db = surge_reasons.get(ticker, {})
        return db.get("reason", "") if isinstance(db, dict) else ""

    df = df.copy()
    df["급등이유"] = df.apply(_reason, axis=1)

    # 표시용 포맷
    df_display = df.rename(columns={
        "rank": "순위", "srtnCd": "종목코드", "itmsNm": "종목명",
        "mrktCtg": "시장", "clpr": "종가", "fltRt": "등락률(%)",
        "trqu": "거래량", "basDt": "기준일",
    })

    selected = st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )
    return selected, df


# ── UI: 종목 상세 ─────────────────────────────────────────────────────────────

def render_stock_detail(ticker: str, name: str, rsi_snapshot: dict, cb_overhang: dict):
    st.divider()
    st.subheader(f"📋 {name} ({ticker}) 상세")

    col1, col2, col3 = st.columns(3)

    # RSI 정보
    rsi_daily = rsi_snapshot.get(f"{ticker}_daily", {})
    rsi_weekly = rsi_snapshot.get(f"{ticker}_1wk", {})
    with col1:
        st.markdown("**RSI (일봉)**")
        if rsi_daily:
            st.metric("RSI", f"{rsi_daily.get('rsi', '-')}", rsi_daily.get('signal', ''))
        else:
            st.caption("데이터 없음")
    with col2:
        st.markdown("**RSI (주봉)**")
        if rsi_weekly:
            st.metric("RSI", f"{rsi_weekly.get('rsi', '-')}", rsi_weekly.get('signal', ''))
        else:
            st.caption("데이터 없음")

    # CB/BW 오버행
    with col3:
        st.markdown("**CB/BW 미상환**")
        overhang = cb_overhang.get(ticker)
        if overhang:
            st.warning("⚠️ 오버행 데이터 있음")
        else:
            st.success("✅ 오버행 데이터 없음")

    # 가격 차트
    st.markdown("**📈 6개월 주가 (yfinance, 15분 지연)**")
    price_df = get_price_history(ticker)
    if not price_df.empty:
        st.line_chart(price_df["Close"])
    else:
        st.caption("차트 데이터 없음 (yfinance)")


# ── DART 공시 ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=1800)
def get_dart_disclosures(ticker: str, days: int = 30) -> list:
    """DART API로 최근 공시 목록 조회."""
    if not DART_API_KEY:
        return []
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=days)
    params = {
        "crtfc_key": DART_API_KEY,
        "stock_code": ticker,
        "bgn_de": start_dt.strftime("%Y%m%d"),
        "end_de": end_dt.strftime("%Y%m%d"),
        "page_count": 20,
    }
    try:
        r = requests.get("https://opendart.fss.or.kr/api/list.json", params=params, timeout=10)
        data = r.json()
        if data.get("status") != "000":
            return []
        return data.get("list", [])
    except Exception as e:
        log.error(f"[dart] {ticker}: {e}")
        return []


def render_dart_tab(ticker: str, name: str):
    st.subheader(f"📋 DART 공시 — {name} ({ticker}), 최근 30일")
    if not ticker:
        st.info("급등주 랭킹에서 종목을 먼저 선택하세요.")
        return
    if not DART_API_KEY:
        st.warning("DART_API_KEY가 설정되지 않았습니다.")
        return

    items = get_dart_disclosures(ticker)
    if not items:
        st.info("최근 30일 공시 없음.")
        return

    _TYPE_LABEL = {
        "A": "정기공시", "B": "주요사항보고", "C": "발행공시",
        "D": "지분공시", "E": "기타공시", "F": "외부감사관련",
        "G": "펀드공시", "H": "자산유동화", "I": "거래소공시", "J": "공정위공시",
    }
    rows = []
    for it in items:
        rows.append({
            "날짜": it.get("rcept_dt", "")[:8],
            "구분": _TYPE_LABEL.get(it.get("pblntf_ty", ""), it.get("pblntf_ty", "")),
            "제목": it.get("report_nm", ""),
            "제출인": it.get("flr_nm", ""),
        })
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)


# ── UI: 테마뉴스 탭 ──────────────────────────────────────────────────────────

def render_theme_news(news_items: list):
    st.subheader("📰 테마뉴스 (infostock, Project 1 수집)")
    if not news_items:
        st.info("테마뉴스 데이터가 없습니다. Project 1 Export를 먼저 실행하세요.")
        return

    for item in news_items[:30]:
        title   = item.get("title", "")
        body    = item.get("body", "")
        date_   = item.get("date", "")[:10] if item.get("date") else ""
        label   = item.get("label", "")
        with st.expander(f"[{date_}] {title}"):
            if label:
                st.caption(label)
            if body:
                st.markdown(body[:500] + ("..." if len(body) > 500 else ""))


# ── 메인 ──────────────────────────────────────────────────────────────────────

def main():
    st.title("📈 급등주 모멘텀 대시보드 (공개)")
    st.caption("합법 데이터 소스 전용: 금융위원회 공공API + DART + yfinance + GitHub 공유 데이터")

    # 사전 로드
    indices      = get_market_indices()
    surge_df     = get_surge_ranking()
    surge_reasons = get_surge_reasons()
    rsi_snapshot = get_rsi_snapshot()
    cb_overhang  = get_cb_overhang()
    theme_news   = get_theme_news()

    render_sidebar(indices)

    # 선택 종목 session_state 유지
    if "selected_ticker" not in st.session_state:
        st.session_state.selected_ticker = ""
        st.session_state.selected_name   = ""

    tab_surge, tab_dart, tab_theme = st.tabs(["🚀 급등주 랭킹", "📋 DART 공시", "📰 테마뉴스"])

    with tab_surge:
        result = render_surge_table(surge_df, surge_reasons)
        if result:
            selected, df = result
            rows = selected.selection.rows if hasattr(selected, "selection") else []
            if rows:
                row = df.iloc[rows[0]]
                st.session_state.selected_ticker = str(row["srtnCd"]).zfill(6)
                st.session_state.selected_name   = row["itmsNm"]
            if st.session_state.selected_ticker:
                render_stock_detail(
                    st.session_state.selected_ticker,
                    st.session_state.selected_name,
                    rsi_snapshot, cb_overhang,
                )

    with tab_dart:
        render_dart_tab(st.session_state.selected_ticker, st.session_state.selected_name)

    with tab_theme:
        render_theme_news(theme_news)


if __name__ == "__main__":
    main()
