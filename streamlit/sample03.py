import streamlit as st
from streamlit_option_menu import option_menu

# 1. ページ全体のレイアウト設定（必ずスクリプトの最初に記述）
st.set_page_config(page_title="Windows風メニューアプリ", layout="wide")

# 2. 上部に水平方向のメニューバーを配置
# orientation="horizontal" を指定することで、上部メニューバーとして機能させます
selected = option_menu(
    menu_title=None,  # メニュータイトルは非表示（Windowsメニュー風にするため）
    options=["File", "Edit", "View", "Help"], # メニュー項目のリスト
    icons=["file-earmark", "pencil-square", "display", "question-circle"], # Bootstrapアイコンのクラス名
    menu_icon="cast", 
    default_index=0,
    orientation="horizontal",
    styles={
        "container": {"padding": "0!important", "background-color": "#f0f2f6"},
        "icon": {"color": "#333", "font-size": "16px"}, 
        "nav-link": {
            "font-size": "14px", 
            "text-align": "center", 
            "margin": "0px", 
            "--hover-color": "#e0e0e0"
        },
        "nav-link-selected": {"background-color": "#4e8df5"},
    }
)

# 3. 選択されたメニューに応じたルーティング（画面の切り替え）
st.write("---") # メニューバーとコンテンツの区切り線

if selected == "File":
    st.title("File メニュー画面")
    st.write("ここに新規作成、保存、開くなどの処理UIを実装します。")
    
    # 疑似的なサブメニューの例
    col1, col2, col3 = st.columns(3)
    with col1:
        st.button("📄 新規作成")
    with col2:
        st.button("💾 保存")
        
elif selected == "Edit":
    st.title("Edit メニュー画面")
    st.write("テキストの編集や設定変更のUIを配置します。")
    st.text_area("エディタエリア", height=200)

elif selected == "View":
    st.title("View メニュー画面")
    st.write("データの可視化やレイアウト変更のUIを配置します。")
    st.bar_chart([10, 20, 30, 40])

elif selected == "Help":
    st.title("Help")
    st.info("このアプリケーションのバージョン情報やヘルプドキュメントを表示します。")
