import csv
from docx import Document

def extract_tables_to_csv(docx_file, output_folder):
    # Wordファイルを読み込む
    document = Document(docx_file)
    
    # テーブルを抽出
    for idx, table in enumerate(document.tables):
        # CSVファイル名を作成（例: table_1.csv, table_2.csv）
        csv_file = f"{output_folder}/table_{idx + 1}.csv"
        
        # CSVファイルに書き出し
        with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            for row in table.rows:
                # セルの内容を取得し、改行を<br>タグに置き換える
                writer.writerow([cell.text.replace('\n', '<br>') for cell in row.cells])
        
        print(f"Table {idx + 1} written to {csv_file}")

# 使用例
# docxファイルのパス
docx_file_path = "example.docx"
# 出力先フォルダのパス
output_folder_path = "output_csv"

# テーブルを抽出してCSVに書き出す
extract_tables_to_csv(docx_file_path, output_folder_path)