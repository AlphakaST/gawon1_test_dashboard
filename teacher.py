from __future__ import annotations
import json
from typing import List, Dict, Any, Tuple, Optional, Set

import streamlit as st
import pandas as pd
from sqlalchemy import text, exc

# ──────────────────────────────────────────
# 페이지 설정
# ──────────────────────────────────────────
st.set_page_config(page_title="교사용 대시보드 — DAT1·2·3 통합", page_icon="📊", layout="wide")
st.title("📊 교사용 대시보드 — DAT1·DAT2·DAT3 통합")
st.caption("학생 제출 내역을 검색/열람하고 CSV로 내려받을 수 있습니다. (멀티 스키마·문항 자동 감지)")

# ──────────────────────────────────────────
# DB 연결
# ──────────────────────────────────────────
try:
    # st.connection은 .streamlit/secrets.toml 또는 Streamlit Share의 Secrets 설정을 자동으로 읽습니다.
    # secrets에 user 대신 username 으로 키를 설정해야 합니다.
    conn = st.connection("mysql", type="sql")
    conn.query("SELECT 1")
    DB_STATUS = "ONLINE"
except Exception as e:
    conn = None
    DB_STATUS = f"OFFLINE: {e}"

st.info(f"DB 상태: {DB_STATUS}")

# ──────────────────────────────────────────
# 유틸: 스키마/테이블/컬럼 탐지
# ──────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=60)
def get_current_schema() -> str:
    """st.secrets에 명시된 기본 데이터베이스 이름을 반환합니다."""
    try:
        return st.secrets.connections.mysql.database
    except Exception:
        return "pr"

@st.cache_data(show_spinner=False, ttl=60)
def get_table_columns(schema: str, table: str) -> Set[str]:
    """특정 테이블의 모든 컬럼 이름을 소문자로 반환합니다."""
    if not conn: return set()
    try:
        query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s"
        df_cols = conn.query(query, params=(schema, table), ttl=60)
        return {str(r[0]).lower() for r in df_cols.itertuples(index=False, name=None)}
    except (exc.SQLAlchemyError, KeyError):
        return set()

@st.cache_data(show_spinner=False, ttl=60)
def list_problem_tables() -> List[Tuple[str, str]]:
    """SHOW TABLES를 사용하여 DAT 테이블 목록을 안정적으로 조회합니다. (수정된 버전)"""
    if not conn:
        return [('pr', "DAT2")]

    try:
        schema_to_check = get_current_schema()
        
        # SQL의 LIKE 대신 Python에서 필터링하여 안정성 향상
        query = f"SHOW TABLES FROM `{schema_to_check}`"
        df_all_tables = conn.query(query, ttl=60)

        if df_all_tables.empty:
            return [('pr', "DAT2")]

        all_table_names = df_all_tables.iloc[:, 0].tolist()
        dat_table_names = [name for name in all_table_names if name.upper().startswith('DAT')]

        if not dat_table_names:
            return [('pr', 'DAT2')]
        
        valid_pairs = []
        for table in dat_table_names:
            cols = get_table_columns(schema_to_check, table)
            if 'id' in cols:
                valid_pairs.append((schema_to_check, table))
        
        if not valid_pairs:
             return [('pr', "DAT2")]

        def keyfn(p: Tuple[str, str]):
            _, t = p
            pri = 0 if t.upper() == "DAT3" else 1 if t.upper() == "DAT2" else 2 if t.upper() == "DAT1" else 3
            return (pri, p[0], t)
        valid_pairs.sort(key=keyfn)
        return valid_pairs

    except (exc.SQLAlchemyError, KeyError, IndexError):
        return [('pr', "DAT2")]


@st.cache_data(show_spinner=False, ttl=60)
def detect_question_count(schema: str, table: str, max_q: int = 4) -> int:
    """테이블의 컬럼을 스캔하여 문항 수(answerN/feedbackN 쌍)를 감지합니다. 0을 반환할 수 있습니다."""
    cols = get_table_columns(schema, table)
    count = 0
    for n in range(1, max_q + 1):
        if f"answer{n}" in cols and f"feedback{n}" in cols:
            count = n
        else:
            break
    return count

@st.cache_data(show_spinner=True, ttl=30)
def fetch_rows(schema: str, table: str, nq: int, keyword: str = "", limit: int = 500) -> List[Dict[str, Any]]:
    if not conn: return []

    all_cols = get_table_columns(schema, table)
    select_list: List[str] = ["id"]
    if "time" in all_cols: select_list.append("time")
    if "opinion1" in all_cols: select_list.append("opinion1")
    for i in range(1, nq + 1):
        if f"answer{i}" in all_cols: select_list.append(f"answer{i}")
        if f"feedback{i}" in all_cols: select_list.append(f"feedback{i}")

    q = f"SELECT {', '.join(select_list)} FROM `{schema}`.`{table}`"
    params: Dict[str, Any] = {"limit": int(limit)}
    where_parts: List[str] = []

    if keyword:
        like_keyword = f"%{keyword}%"
        params["keyword"] = like_keyword
        if "id" in all_cols: where_parts.append("id LIKE :keyword")
        if "opinion1" in all_cols: where_parts.append("opinion1 LIKE :keyword")
        for i in range(1, nq + 1):
            if f"answer{i}" in all_cols: where_parts.append(f"answer{i} LIKE :keyword")
            if f"feedback{i}" in all_cols: where_parts.append(f"feedback{i} LIKE :keyword")

    if where_parts:
        q += " WHERE " + " OR ".join(where_parts)

    q += f" ORDER BY {'time DESC' if 'time' in all_cols else 'id DESC'} LIMIT :limit"

    try:
        df_rows = conn.query(q, params=params, ttl=30)
        return df_rows.to_dict("records")
    except (exc.SQLAlchemyError, KeyError) as e:
        st.error(f"[DB] 조회 실패: {e}")
        return []

# ──────────────────────────────────────────
# 데이터 가공 (JSON 피드백 파싱)
# ──────────────────────────────────────────

def _json_parse(txt: str) -> Optional[Dict[str, Any]]:
    if not txt: return None
    try: return json.loads(txt)
    except Exception: return None

def parse_feedback_generic(text: str, qidx: int, table_name: str = "") -> Dict[str, Any]:
    res = {"score": None, "max": None, "level": None, "feedback": "", "detected": {}, "reason": ""}
    if not text: return res
    data = _json_parse(text)
    if not isinstance(data, dict):
        res["reason"] = "JSON 파싱 실패"; res["feedback"] = (text or "")[:500]
        return res
    res["feedback"] = data.get("feedback", "")
    res["detected"] = data.get("detected", {})
    if "score" in data or "max" in data:
        res["score"] = data.get("score"); res["max"] = data.get("max")
    if "level" in data:
        lv = str(data.get("level") or "").upper()
        res["level"] = lv if lv in {"A","B","C","D"} else None
    if "reason" in data and isinstance(data.get("reason"), str):
        res["reason"] = data.get("reason")
    if table_name.upper() == "DAT2":
        flags = {}
        if qidx == 1: flags = {"응고/얼음": bool(res["detected"].get("freezing")), "열 방출(응고열)": bool(res["detected"].get("heat_release"))}
        elif qidx == 2: flags = {"공통: 열 흡수": bool(res["detected"].get("heat_absorb_common")), "차이: 승화": bool(res["detected"].get("sublimation")), "차이: 융해": bool(res["detected"].get("fusion"))}
        else: flags = {"㉠ 잠열/상변화 에너지": bool(res["detected"].get("phase_change_energy")),"㉡ 발화점/연소 위험": bool(res["detected"].get("ignition_point"))}
        res["detected"] = flags
    return res

def to_dataframe(rows: List[Dict[str, Any]], nq: int, table_name: str) -> pd.DataFrame:
    records = []
    for r in rows:
        row: Dict[str, Any] = {
            "제출시각": r.get("time"),
            "학번": r.get("id"),
            "의견(요약)": (r.get("opinion1") or "")[:120] if "opinion1" in r else "",
        }
        total, total_max, has_any_score = 0, 0, False
        for i in range(1, nq + 1):
            pf = parse_feedback_generic(r.get(f"feedback{i}") or "", i, table_name)
            row[f"점수{i}"] = pf["score"]; row[f"만점{i}"] = pf["max"]
            if pf["score"] is not None:
                has_any_score = True
                try:
                    total += int(pf["score"])
                    total_max += int(pf["max"]) if pf["max"] is not None else 0
                except (ValueError, TypeError): pass
            row[f"성취{i}"] = pf["level"]
            row[f"피드백{i}(요약)"] = (pf["feedback"] or "")[:120]
            row[f"_answer{i}"] = r.get(f"answer{i}", "")
            row[f"_feedback{i}"] = r.get(f"feedback{i}", "")
            row[f"_reason{i}"] = pf.get("reason", "")
            row[f"_flags{i}"] = pf.get("detected", {})
        row["총점"] = total if has_any_score else None
        row["총점_만점"] = total_max if (has_any_score and total_max) else None
        records.append(row)

    df = pd.DataFrame.from_records(records)
    if not df.empty and "제출시각" in df.columns:
        df.sort_values(by="제출시각", ascending=False, inplace=True, ignore_index=True)
    elif not df.empty and "학번" in df.columns:
        df.sort_values(by="학번", ascending=False, inplace=True, ignore_index=True)
    return df

# ──────────────────────────────────────────
# UI — 사이드바(테이블/검색/표시개수)
# ──────────────────────────────────────────
with st.sidebar:
    st.subheader("🗂️ 데이터 소스 & 조회 옵션")
    tables = list_problem_tables()
    labels = [f"{sch}.{tbl}" for sch, tbl in tables]
    def default_index() -> int:
        for pref in ("DAT3", "DAT2", "DAT1"):
            for i, (_, t) in enumerate(tables):
                if t.upper() == pref: return i
        return 0
    idx = st.selectbox("데이터 소스(스키마.테이블)", range(len(labels)), format_func=lambda i: labels[i], index=min(default_index(), len(labels)-1))
    
    sel_schema, sel_table = tables[idx] if tables else ("pr", "DAT2")
        
    nq = detect_question_count(sel_schema, sel_table)
    st.caption(f"선택: **{sel_schema}.{sel_table}** · 자동 감지 문항 수: **{nq}**")

    kw = st.text_input("학번/키워드 검색", placeholder="예: 10130 또는 '승화'")
    limit = st.number_input("표시 개수(최대 2000)", 10, 2000, 500, 10)
    if st.button("새로고침"): st.rerun()

# ──────────────────────────────────────────
# 본문 — 표/상세/다운로드
# ──────────────────────────────────────────
rows = fetch_rows(schema=sel_schema, table=sel_table, nq=nq, keyword=kw.strip(), limit=int(limit))
df = to_dataframe(rows, nq, sel_table)

# [수정] 데이터 건수 표시 부분에 (최대 표시 건수) 안내 추가
st.markdown(f"**총 {len(df)}건**의 결과가 있습니다. (최대 **{limit}**건까지 표시)")


main_cols: List[str] = [c for c in ["제출시각", "학번", "총점", "총점_만점"] if c in df.columns]
for i in range(1, nq + 1):
    for c in (f"점수{i}", f"만점{i}", f"성취{i}"):
        if c in df.columns and not df[c].isnull().all():
             main_cols.append(c)
if "의견(요약)" in df.columns and df["의견(요약)"].str.strip().any():
    main_cols.append("의견(요약)")

st.dataframe(df[main_cols] if not df.empty else pd.DataFrame(columns=main_cols), use_container_width=True, height=480)

st.divider()
st.subheader("🧾 상세 보기(학번 입력)")
qid = st.text_input("학번 입력", placeholder="예: 10130", key="detail_id")
if qid:
    if df.empty or "학번" not in df.columns:
        st.info("조회된 데이터가 없어 학번을 검색할 수 없습니다.")
    else:
        sub = df[df["학번"].astype(str) == qid.strip()]
        if sub.empty:
            st.info("해당 학번의 제출 내역이 없습니다.")
        else:
            row = sub.iloc[0]
            if nq > 0:
                tab_names = [f"문항 {i}" for i in range(1, nq + 1)]
                if nq == 4: tab_names = ["문항 1", "문항 2-1", "문항 2-2", "문항 3"]
                tabs = st.tabs(tab_names)
                for i, tb in enumerate(tabs, start=1):
                    with tb:
                        c1, c2 = st.columns(2)
                        with c1:
                            if reason := row.get(f"_reason{i}", ""):
                                st.markdown("**채점 근거**"); st.write(reason)
                            st.markdown("**피드백(전문)**")
                            try:
                                raw_fb = row.get(f"_feedback{i}", "{}")
                                raw = json.loads(raw_fb if raw_fb and raw_fb.strip() else "{}")
                                st.write(raw.get("feedback", ""))
                                if lv := raw.get("level"): st.caption(f"성취수준: {lv}")
                            except Exception:
                                st.write(row.get(f"피드백{i}(요약)", ""))
                        with c2:
                            st.markdown("**학생 답안(전문)**"); st.write(row.get(f"_answer{i}", ""))
                            st.markdown("**조건 충족(탐지)**")
                            if flags := row.get(f"_flags{i}"):
                                for k, v in flags.items():
                                    st.write(f"{k}: {'✅' if isinstance(v, bool) and v else '❌' if isinstance(v, bool) else v}")
                            else:
                                st.write("-")
            else:
                st.info("이 테이블에는 상세 분석할 문항(답안/피드백)이 없습니다.")

            if "의견(요약)" in row and pd.notna(row["의견(요약)"]) and row["의견(요약)"].strip():
                st.markdown("**학생 의견(전문)**"); st.write(row.get("의견(요약)"))

st.divider()
st.subheader("⬇️ CSV 다운로드")
if not df.empty:
    csv_df = df[[c for c in df.columns if not c.startswith("_")]].copy()
    st.download_button("CSV로 저장하기", csv_df.to_csv(index=False).encode("utf-8-sig"), f"{sel_schema}.{sel_table}_dashboard.csv", "text/csv")
else:
    st.info("다운로드할 데이터가 없습니다.")

