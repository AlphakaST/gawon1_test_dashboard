# teacher_dashboard_fixed.py — DAT1·2·3 통합 대시보드 (연결 문제 해결)
# -*- coding: utf-8 -*-
"""
기능 요약 (기존과 동일)
- 하나의 대시보드에서 DAT1, DAT2, DAT3 테이블을 모두 조회.
- answerN/feedbackN 열을 자동 탐지하여 문항 수 자동 감지.
- DAT1/2(점수형)와 DAT3(성취수준 A–D) JSON을 모두 파싱하여 표/상세 탭 제공.
- 키워드 검색, CSV 다운로드, 상세 학번 보기.

수정 사항
- DB 연결 방식을 Streamlit Cloud에 최적화된 `st.connection`으로 변경하여 연결 오류 해결.
- DB 조회 관련 함수들을 `st.connection`의 `conn.query()` 메소드에 맞게 재작성.
- SQL 인젝션 방지를 위해 파라미터 바인딩 방식을 SQLAlchemy 스타일(:param)로 통일.
"""
from __future__ import annotations
import json
from typing import List, Dict, Any, Tuple, Optional, Set

import streamlit as st
import pandas as pd
# --------------------------------------------------------------------------
# 수정 1: SQLAlchemy의 text 함수를 임포트하고 mysql.connector는 제거합니다.
# --------------------------------------------------------------------------
from sqlalchemy import text, exc

# ──────────────────────────────────────────
# 페이지 설정
# ──────────────────────────────────────────
st.set_page_config(page_title="교사용 대시보드 — DAT1·2·3 통합", page_icon="📊", layout="wide")
st.title("📊 교사용 대시보드 — DAT1·DAT2·DAT3 통합")
st.caption("학생 제출 내역을 검색/열람하고 CSV로 내려받을 수 있습니다. (멀티 스키마·문항 자동 감지)")

# ──────────────────────────────────────────
# 수정 2: DB 연결 방식을 st.connection으로 변경
# ──────────────────────────────────────────
try:
    db_creds = st.secrets.connections.mysql
    conn = st.connection(
        "mysql", type="sql", dialect="mysql",
        host=db_creds.host, port=db_creds.port, database=db_creds.database,
        username=db_creds.user, password=db_creds.password
    )
    conn.query("SELECT 1") # 연결 테스트
    DB_STATUS = "ONLINE"
except Exception as e:
    conn = None
    DB_STATUS = f"OFFLINE: {e}"
st.info(f"DB 상태: {DB_STATUS}")


# ──────────────────────────────────────────
# 수정 3: 모든 DB 조회 함수를 conn.query() 기반으로 재작성
# ──────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=60)
def get_current_schema() -> str:
    if not conn: return ""
    try:
        df = conn.query("SELECT DATABASE()")
        return df.iloc[0, 0] if not df.empty else ""
    except exc.SQLAlchemyError:
        return ""

@st.cache_data(show_spinner=False, ttl=60)
def schema_exists(schema: str) -> bool:
    if not conn: return False
    try:
        df = conn.query("SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME=:schema", params={"schema": schema})
        return (df.iloc[0,0] > 0) if not df.empty else False
    except exc.SQLAlchemyError:
        return False

@st.cache_data(show_spinner=False, ttl=60)
def get_table_columns(schema: str, table: str) -> Set[str]:
    if not conn: return set()
    try:
        df = conn.query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=:schema AND TABLE_NAME=:table",
            params={"schema": schema, "table": table}
        )
        return {r.lower() for r in df['COLUMN_NAME']}
    except (exc.SQLAlchemyError, KeyError):
        return set()

@st.cache_data(show_spinner=False, ttl=60)
def list_problem_tables() -> List[Tuple[str, str]]:
    if not conn: return [(get_current_schema(), "DAT2")] # 기본값
    try:
        current = get_current_schema()
        schemas_to_check = [s for s in {current, "pr"} if s and schema_exists(s)]
        if not schemas_to_check: return [(current, "DAT2")]

        # SQLAlchemy는 IN 절에 리스트를 직접 바인딩할 수 있습니다.
        sql = """
            SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA IN :schemas AND COLUMN_NAME IN ('id','answer1','feedback1')
            GROUP BY TABLE_SCHEMA, TABLE_NAME HAVING COUNT(DISTINCT COLUMN_NAME)=3
        """
        df = conn.query(sql, params={"schemas": schemas_to_check})
        pairs = list(zip(df['TABLE_SCHEMA'], df['TABLE_NAME']))

        def keyfn(p: Tuple[str,str]):
            _, t = p
            pri = 0 if t.upper()=="DAT3" else 1 if t.upper()=="DAT2" else 2 if t.upper()=="DAT1" else 3
            return (pri, p[0], t)
        pairs.sort(key=keyfn)
        return pairs or [(current, "DAT2")]
    except (exc.SQLAlchemyError, KeyError):
        return [(get_current_schema(), "DAT2")]

@st.cache_data(show_spinner=False, ttl=60)
def detect_question_count(schema: str, table: str, max_q: int = 4) -> int:
    """테이블의 컬럼을 스캔하여 문항 수(answerN/feedbackN 쌍)를 감지합니다."""
    cols = get_table_columns(schema, table)
    count = 0
    for n in range(1, max_q + 1):
        if f"answer{n}" in cols and f"feedback{n}" in cols:
            count = n
    return max(1, min(count, max_q))

@st.cache_data(show_spinner=True, ttl=30)
def fetch_rows(schema: str, table: str, nq: int, keyword: str = "", limit: int = 500) -> List[Dict[str, Any]]:
    if not conn: return []

    all_cols = get_table_columns(schema, table)
    select_list: List[str] = ["id"]
    if "time" in all_cols: select_list.append("time")
    if "opinion1" in all_cols: select_list.append("opinion1")
    for i in range(1, nq+1):
        if f"answer{i}" in all_cols: select_list.append(f"answer{i}")
        if f"feedback{i}" in all_cols: select_list.append(f"feedback{i}")

    select_clause = ", ".join(select_list)
    q = f"SELECT {select_clause} FROM `{schema}`.`{table}`"
    params: Dict[str, Any] = {}
    
    where_parts: List[str] = []
    if keyword:
        like_keyword = f"%{keyword}%"
        params["like_keyword"] = like_keyword
        where_parts.append("id LIKE :like_keyword")
        if "opinion1" in select_list: where_parts.append("opinion1 LIKE :like_keyword")
        for i in range(1, nq+1):
            if f"answer{i}" in select_list: where_parts.append(f"answer{i} LIKE :like_keyword")
            if f"feedback{i}" in select_list: where_parts.append(f"feedback{i} LIKE :like_keyword")

    if where_parts:
        q += " WHERE " + " OR ".join(where_parts)

    q += " ORDER BY time DESC" if "time" in all_cols else " ORDER BY id DESC"
    q += " LIMIT :limit"
    params["limit"] = int(limit)

    try:
        df = conn.query(q, params=params)
        return df.to_dict('records') # DataFrame을 list of dicts로 변환
    except exc.SQLAlchemyError as e:
        st.error(f"[DB] 조회 실패: {e}")
        return []


# --------------------------------------------------------------------------
# 이하 데이터 가공 및 UI 코드는 기존과 거의 동일 (수정 불필요)
# --------------------------------------------------------------------------

def _json_parse(txt: str) -> Optional[Dict[str, Any]]:
    if not txt: return None
    try:
        return json.loads(txt)
    except Exception:
        return None


def parse_feedback_generic(text: str, qidx: int, table_name: str = "") -> Dict[str, Any]:
    res = {"score": None, "max": None, "level": None, "feedback": "", "detected": {}, "reason": ""}
    if not text:
        return res
    data = _json_parse(text)
    if not isinstance(data, dict):
        res["reason"] = "JSON 파싱 실패"; res["feedback"] = (text or "")[:500]
        return res

    if "feedback" in data and isinstance(data.get("feedback"), str):
        res["feedback"] = data.get("feedback", "")
    if "detected" in data and isinstance(data.get("detected"), dict):
        res["detected"] = data.get("detected", {})
    if "score" in data or "max" in data:
        res["score"] = data.get("score")
        res["max"] = data.get("max")
    if "level" in data:
        lv = str(data.get("level") or "").upper()
        res["level"] = lv if lv in {"A","B","C","D"} else None
    if "reason" in data and isinstance(data.get("reason"), str):
        res["reason"] = data.get("reason")

    if table_name.upper() == "DAT2":
        if qidx == 1:
            flags = {
                "응고/얼음": bool(res["detected"].get("freezing")),
                "열 방출(응고열)": bool(res["detected"].get("heat_release")),
            }
        elif qidx == 2:
            flags = {
                "공통: 열 흡수": bool(res["detected"].get("heat_absorb_common")),
                "차이: 승화": bool(res["detected"].get("sublimation")),
                "차이: 융해": bool(res["detected"].get("fusion")),
            }
        else:
            flags = {
                "㉠ 잠열/상변화 에너지": bool(res["detected"].get("phase_change_energy")),
                "㉡ 발화점/연소 위험": bool(res["detected"].get("ignition_point")),
            }
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
        total, total_max = 0, 0
        has_any_score = False
        for i in range(1, nq+1):
            pf = parse_feedback_generic(r.get(f"feedback{i}") or "", i, table_name)
            row[f"점수{i}"] = pf["score"]
            row[f"만점{i}"] = pf["max"]
            if pf["score"] is not None:
                has_any_score = True
                try:
                    total += int(pf["score"]) if pf["score"] is not None else 0
                    total_max += int(pf["max"]) if pf["max"] is not None else 0
                except Exception:
                    pass
            row[f"성취{i}"] = pf["level"]
            fb_text = pf["feedback"] or ""
            row[f"피드백{i}(요약)"] = fb_text[:120]
            row[f"_answer{i}"] = r.get(f"answer{i}") or ""
            row[f"_feedback{i}"] = r.get(f"feedback{i}") or ""
            row[f"_reason{i}"] = pf.get("reason", "")
            row[f"_flags{i}"] = pf.get("detected", {})
        row["총점"] = total if has_any_score else None
        row["총점_만점"] = total_max if (has_any_score and total_max) else None
        records.append(row)

    df = pd.DataFrame.from_records(records)
    if not df.empty:
        sort_cols = [c for c in ["제출시각", "학번"] if c in df.columns]
        if sort_cols:
            df.sort_values(by=sort_cols, ascending=[False, True][:len(sort_cols)], inplace=True, ignore_index=True)
    return df

# ──────────────────────────────────────────
# UI — 사이드바
# ──────────────────────────────────────────
with st.sidebar:
    st.subheader("🗂️ 데이터 소스 & 조회 옵션")
    tables = list_problem_tables()
    labels = [f"{sch}.{tbl}" for sch, tbl in tables]
    def default_index() -> int:
        for pref in ("DAT3", "DAT2", "DAT1"):
            for i, (_, t) in enumerate(tables):
                if t.upper() == pref:
                    return i
        return 0
    idx = st.selectbox("데이터 소스(스키마.테이블)", options=list(range(len(labels))), format_func=lambda i: labels[i], index=min(default_index(), len(labels)-1))
    sel_schema, sel_table = tables[idx]
    nq = detect_question_count(sel_schema, sel_table)
    st.caption(f"선택: **{sel_schema}.{sel_table}** · 자동 감지 문항 수: **{nq}**")

    kw = st.text_input("학번/키워드 검색", placeholder="예: 10130 또는 '승화'")
    limit = st.number_input("표시 개수(최대 2000)", min_value=10, max_value=2000, value=500, step=10)
    col_sb1, col_sb2 = st.columns(2)
    with col_sb1:
        if st.button("새로고침"):
            st.rerun()
    with col_sb2:
        show_raw = st.checkbox("원문 열 포함(다운로드용)", value=False)

# ──────────────────────────────────────────
# 본문 — 표/상세/다운로드
# ──────────────────────────────────────────
if DB_STATUS != "ONLINE":
    st.error("DB가 오프라인 상태입니다. secrets 정보를 확인하거나 관리자에게 문의하세요.")
    st.stop()

rows = fetch_rows(schema=sel_schema, table=sel_table, nq=nq, keyword=kw.strip(), limit=int(limit))
df = to_dataframe(rows, nq, sel_table)

st.markdown(f"**총 {len(df)}건** 결과가 있습니다.")

main_cols: List[str] = []
for c in ["제출시각", "학번", "총점", "총점_만점"]:
    if c in df.columns:
        main_cols.append(c)
for i in range(1, nq+1):
    for c in (f"점수{i}", f"만점{i}", f"성취{i}"):
        if c in df.columns:
            main_cols.append(c)
if "의견(요약)" in df.columns:
    main_cols.append("의견(요약)")

st.dataframe(
    df[main_cols] if not df.empty else pd.DataFrame(columns=main_cols),
    use_container_width=True, height=480
)

st.divider()
st.subheader("🧾 상세 보기(학번 입력)")
qid = st.text_input("학번 입력", placeholder="예: 10130", key="detail_id")
if qid:
    sub = df[df["학번"].astype(str) == qid.strip()]
    if sub.empty:
        st.info("해당 학번의 제출 내역이 없습니다.")
    else:
        row = sub.iloc[0]
        if nq == 4:
            tab_names = ["문항 1", "문항 2-1", "문항 2-2", "문항 3"]
        else:
            tab_names = [f"문항 {i}" for i in range(1, nq+1)]
        tabs = st.tabs(tab_names)
        for i, tb in enumerate(tabs, start=1):
            with tb:
                c1, c2 = st.columns(2)
                with c1:
                    reason = row.get(f"_reason{i}", "")
                    if reason:
                        st.markdown("**채점 근거**"); st.write(reason)
                    st.markdown("**피드백(전문)**")
                    try:
                        raw = json.loads(row.get(f"_feedback{i}", ""))
                        st.write(raw.get("feedback", ""))
                        lv = raw.get("level")
                        if isinstance(lv, str) and lv.upper() in {"A","B","C","D"}:
                            st.caption(f"성취수준: {lv}")
                    except Exception:
                        st.write(row.get(f"피드백{i}(요약)", ""))
                with c2:
                    st.markdown("**학생 답안(전문)**"); st.write(row.get(f"_answer{i}", ""))
                    st.markdown("**조건 충족(탐지)**")
                    flags = row.get(f"_flags{i}") or {}
                    if not flags:
                        st.write("-")
                    else:
                        for k, v in flags.items():
                            if isinstance(v, bool):
                                st.write(f"{k}: {'✅' if v else '❌'}")
                            else:
                                st.write(f"{k}: {v}")
        if "의견(요약)" in row:
            st.markdown("**학생 의견(요약)**"); st.write(row.get("의견(요약)", ""))

st.divider()
st.subheader("⬇️ CSV 다운로드")
if not df.empty:
    csv_df = df.copy() if show_raw else df[[c for c in df.columns if not c.startswith("_")]].copy()
    st.download_button(
        "CSV로 저장하기",
        data=csv_df.to_csv(index=False).encode("utf-8-sig"),
        file_name=f"{sel_schema}.{sel_table}_dashboard.csv",
        mime="text/csv",
    )
else:
    st.info("다운로드할 데이터가 없습니다.")

st.caption("Tip) 좌측에서 스키마.테이블을 바꾸면 DAT1/2/3 데이터를 한 화면에서 조회할 수 있습니다.")

