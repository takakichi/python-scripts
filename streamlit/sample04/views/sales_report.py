import streamlit as st
import duckdb
import pandas as pd
import os

PARQUET_FILE = "sample_data.parquet"

def _generate_dummy_parquet():
    """動作確認用のParquetファイルが存在しない場合に生成するヘルパー関数"""
    if not os.path.exists(PARQUET_FILE):
        df = pd.DataFrame({
            "date": pd.date_range(start="2024-01-01", periods=1000, freq="h"),
            "category": ["Electronics", "Clothing", "Food", "Books"] * 250,
            "sales_amount": [5000, 3000, 800, 1500] * 250
        })
        df.to_parquet(PARQUET_FILE)

def render():
    st.title("売上分析レポート")
    _generate_dummy_parquet()

    # --- 状態保持（検索条件）の設定 ---
    # 初回アクセス時のデフォルト値をセッションに登録
    if "filter_category" not in st.session_state:
        st.session_state.filter_category = "All"
    if "filter_min_sales" not in st.session_state:
        st.session_state.filter_min_sales = 0

    st.subheader("検索条件")
    col1, col2 = st.columns(2)
    
    with col1:
        # keyを指定することで、変更時に自動的にst.session_state.filter_categoryが更新される
        st.selectbox(
            "カテゴリ",
            ["All", "Electronics", "Clothing", "Food", "Books"],
            key="filter_category"
        )
    with col2:
        st.number_input(
            "最小売上額",
            min_value=0,
            step=100,
            key="filter_min_sales"
        )

    # --- DuckDBによるParquetファイルの直接クエリ ---
    # ユーザーの検索条件（セッションステート）をクエリに反映
    selected_category = st.session_state.filter_category
    min_sales = st.session_state.filter_min_sales

    # パラメータ化クエリの構築
    where_clauses = ["sales_amount >= ?"]
    params = [min_sales]

    if selected_category != "All":
        where_clauses.append("category = ?")
        params.append(selected_category)
        
    where_sql = " AND ".join(where_clauses)

    query = f"""
        SELECT 
            category,
            COUNT(*) as tx_count,
            SUM(sales_amount) as total_sales,
            AVG(sales_amount) as avg_sales
        FROM '{PARQUET_FILE}'
        WHERE {where_sql}
        GROUP BY category
        ORDER BY total_sales DESC
    """

    try:
        # DuckDBでクエリ実行（Parquetファイルを直接スキャンするため高速）
        with duckdb.connect() as con:
            result_df = con.execute(query, params).df()

        st.dataframe(result_df, use_container_width=True)
        
        if not result_df.empty:
            st.bar_chart(result_df.set_index("category")["total_sales"])

    except Exception as e:
        st.error(f"データ取得エラー: {e}")