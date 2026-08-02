import streamlit as st
from views import home, sales_report

st.set_page_config(page_title="Data App", layout="wide")

# ルーティング用の状態初期化
if "current_page" not in st.session_state:
    st.session_state.current_page = "Home"

def navigate_to(page_name):
    st.session_state.current_page = page_name

# --- プルダウンメニューの実装 ---
cols = st.columns([1, 1, 10])

with cols[0]:
    with st.popover("📁 File", use_container_width=True):
        st.button("Home", on_click=navigate_to, args=("Home",), use_container_width=True)
with cols[1]:
    with st.popover("📊 Data", use_container_width=True):
        st.button("売上分析 (Parquet)", on_click=navigate_to, args=("SalesReport",), use_container_width=True)

st.divider()

# --- ルーティング ---
page = st.session_state.current_page

if page == "Home":
    home.render()
elif page == "SalesReport":
    sales_report.render()