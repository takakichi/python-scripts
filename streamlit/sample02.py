import streamlit as st
import sqlite3
from datetime import datetime

DB_NAME = "survey.db"

# DB初期化
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS survey (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            satisfaction INTEGER,
            comment TEXT,
            created_at TEXT
        )
    """)
    conn.commit()
    conn.close()

# データ保存
def save_survey(name, satisfaction, comment):
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO survey (name, satisfaction, comment, created_at)
        VALUES (?, ?, ?, ?)
    """, (name, satisfaction, comment, datetime.now().isoformat()))
    conn.commit()
    conn.close()

# データ取得
def load_survey():
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()
    cur.execute("SELECT name, satisfaction, comment, created_at FROM survey")
    rows = cur.fetchall()
    conn.close()
    return rows

# -----------------------
# Streamlit UI
# -----------------------
st.title("アンケートフォーム")

init_db()

with st.form("survey_form"):
    name = st.text_input("お名前")
    satisfaction = st.slider("満足度", 1, 5, 3)
    comment = st.text_area("コメント")
    submitted = st.form_submit_button("送信")

if submitted:
    if name:
        save_survey(name, satisfaction, comment)
        st.success("アンケートを保存しました")
    else:
        st.warning("お名前を入力してください")

st.divider()

st.subheader("アンケート結果一覧")

data = load_survey()
if data:
    st.dataframe(
        data,
        column_config={
            0: "名前",
            1: "満足度",
            2: "コメント",
            3: "登録日時"
        }
    )
else:
    st.write("まだデータがありません")
