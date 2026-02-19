import pandas as pd
import glob
import os
import yaml
import argparse
import logging

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path):
    """YAMLファイルから設定を読み込む"""
    if not os.path.exists(config_path):
        logging.warning(f"設定ファイル {config_path} が見つかりません。デフォルト設定を使用します。")
        return {}
    
    with open(config_path, 'r', encoding='utf-8') as f:
        try:
            return yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            logging.error(f"YAMLの解析に失敗しました: {e}")
            return {}

def process_files(file_list, exclude_apps):
    """ファイルを1つずつ処理してメモリ消費を抑える"""
    dfs = []
    
    # Windowsイベントログのパス形式 (D:\Application\AppName\...) に合わせた正規表現
    # バックスラッシュが続くため raw string (r'') を使用
    pattern = r'D:\\Application\\([^\\]+)'

    for file_path in file_list:
        try:
            # 必要な列のみ読み込む (メモリ節約)
            df = pd.read_csv(file_path, encoding='utf-8', usecols=['日時', 'メッセージ'])
            
            # Pandasのベクトル処理で高速に抽出
            # expand=False にすることで DataFrame ではなく Series (AppName列) として取得
            df['AppName'] = df['メッセージ'].str.extract(pattern, expand=False)
            
            # アプリ名が抽出できなかった行（パス形式が一致しない行）は除外
            df.dropna(subset=['AppName'], inplace=True)
            
            # 読み込み段階で除外フィルタ適用（不要なデータをメモリに残さない）
            if exclude_apps:
                df = df[~df['AppName'].isin(exclude_apps)]
            
            if not df.empty:
                dfs.append(df)
                
        except Exception as e:
            logging.error(f"ファイル読み込みエラー ({os.path.basename(file_path)}): {e}")
            continue

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

def main():
    parser = argparse.ArgumentParser(description='ログ集計スクリプト')
    parser.add_argument('--log_dir', default=r'D:\Application\logs', help='ログディレクトリのパス')
    parser.add_argument('--config', default='ignore.yaml', help='除外設定ファイル')
    parser.add_argument('--output_csv', default='error_summary.csv', help='出力CSVファイル名')
    parser.add_argument('--output_json', default='error_summary.json', help='出力JSONファイル名')
    args = parser.parse_args()

    # 1. 設定の読み込み
    config_data = load_config(args.config)
    exclude_apps = config_data.get('exclude_apps', [])

    # 2. ログファイルの検索
    search_path = os.path.join(args.log_dir, "log_*.csv")
    all_files = glob.glob(search_path)
    
    if not all_files:
        logging.error(f"処理対象のCSVファイルが見つかりません: {search_path}")
        return

    logging.info(f"{len(all_files)} 個のファイルを処理開始...")

    # 3. ファイル読み込み・結合・フィルタリング
    df_result = process_files(all_files, exclude_apps)

    if df_result.empty:
        logging.warning("集計対象のデータがありませんでした。")
        return

    # 4. 日時整形と集計
    try:
        df_result['DateTime'] = pd.to_datetime(df_result['日時'])
        # 1時間単位で丸める
        df_result['TimeBucket'] = df_result['DateTime'].dt.floor('h')

        # 集計: AppName, TimeBucket, メッセージ ごとの件数
        summary = df_result.groupby(['AppName', 'TimeBucket', 'メッセージ']).size().reset_index(name='Count')

        # 5. 出力
        summary.to_csv(args.output_csv, index=False, encoding='utf-8-sig')
        
        # JSON用に日時を文字列変換
        summary_json = summary.copy()
        summary_json['TimeBucket'] = summary_json['TimeBucket'].dt.strftime('%Y-%m-%d %H:%M:%S')
        summary_json.to_json(args.output_json, orient='records', force_ascii=False, indent=4)

        logging.info(f"集計完了: {len(summary)} 件の集計結果を出力しました。")

    except Exception as e:
        logging.error(f"集計処理中にエラーが発生しました: {e}")

if __name__ == "__main__":
    main()