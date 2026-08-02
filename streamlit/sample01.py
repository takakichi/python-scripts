import streamlit as st
import duckdb
import pandas as pd
import numpy as np
import os

# ==========================================
# 0. 準備：ダミーのParquetファイルを作成（テスト用）
# ==========================================
PARQUET_FILE = "sample_sales.parquet"

@st.cache_data
def create_dummy_parquet():
    if not os.path.exists(PARQUET_FILE):
        # 1万件のランダムな売上データを作成
        dates = pd.date_range(start="2023-01-01", end="2023-12-31", freq="D")
        df = pd.DataFrame({
            "sales_date": np.random.choice(dates, 10000),
            "category": np.random.choice(["Electronics", "Books", "Clothing", "Food"], 10000),
            "region": np.random.choice(["Tokyo", "Osaka", "Fukuoka", "Sapporo"], 10000),
            "sales_amount": np.random.randint(1000, 50000, 10000)
        })
        # Parquet形式で保存（圧縮されて軽量になる）
        df.to_parquet(PARQUET_FILE)

create_dummy_parquet()

# ==========================================
# 1. バックエンド：DuckDBコネクションの管理
# ==========================================
# プロセス全体で1つのインメモリDBコネクションを使い回す
@st.cache_resource
def get_db_connection():
    return duckdb.connect(database=':memory:')

conn = get_db_connection()

# ==========================================
# 2. フロントエンド：UIの構築
# ==========================================
st.title("Parquet 月次集計ダッシュボード")
st.markdown("DuckDBがバックエンドでParquetファイルを直接クエリしているぞ。")

# サイドバーでユーザーに集計軸を選ばせる
st.sidebar.header("集計設定")
group_by_col = st.sidebar.selectbox(
    "どの項目で集計する？",
    ["category", "region"],
    index=0
)

# ==========================================
# 3. データ処理：DuckDBで動的クエリの実行
# ==========================================
# strftimeで年月(YYYY-MM)にフォーマットし、選択された項目でGROUP BYする
query = f"""
    SELECT 
        strftime(sales_date, '%Y-%m') AS month,
        {group_by_col} AS segment,
        SUM(sales_amount) AS total_sales
    FROM '{PARQUET_FILE}'
    GROUP BY 1, 2
    ORDER BY 1, 2
"""

# クエリを実行し、結果だけをPandasのDataFrameとして受け取る
# ※元の重いParquetデータをメモリに乗せないのがポイント
result_df = conn.execute(query).df()


# ==========================================
# 4. 描画：グラフと表の表示
# ==========================================
st.subheader(f"月別売上推移（{group_by_col}別）")

# Streamlitの標準チャート。colorパラメータを指定することで自動的にスタック/色分けされる
st.bar_chart(
    result_df,
    x="month",
    y="total_sales",
    color="segment",
    use_container_width=True
)

st.subheader("集計データ（生テーブル）")
# Streamlitのdataframeはソートやリサイズが標準で可能
st.dataframe(result_df, use_container_width=True, hide_index=True)
