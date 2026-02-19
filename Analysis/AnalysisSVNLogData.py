import argparse
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional, Set

@dataclass
class FilterCriteria:
    """フィルタリング条件を管理するデータクラス"""
    exclude_dirs: Set[str] = field(default_factory=set)
    extensions: Set[str] = field(default_factory=set)
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None

    def __post_init__(self):
        # 高速な検索のために正規化を行う
        self.exclude_dirs = {d if d.startswith('/') else f'/{d}' for d in self.exclude_dirs}
        # 拡張子は小文字かつドット付きに統一
        self.extensions = {ext.lower() if ext.startswith('.') else f'.{ext.lower()}' for ext in self.extensions}

    def is_date_in_range(self, dt: datetime) -> bool:
        if self.start_date and dt < self.start_date:
            return False
        if self.end_date and dt > self.end_date:
            return False
        return True

    def is_path_allowed(self, file_path: str) -> bool:
        if not file_path:
            return False
        
        # 除外ディレクトリの判定
        if any(file_path.startswith(d) for d in self.exclude_dirs):
            return False
        
        # 拡張子の判定（指定がある場合のみ）
        if self.extensions:
            if not any(file_path.lower().endswith(ext) for ext in self.extensions):
                return False
        
        return True

def parse_iso_date(date_str: str) -> datetime:
    """SVNの日付文字列（ISO 8601）をUTC datetimeに変換"""
    # Python 3.7+ fromisoformat は 'Z' をそのまま扱えない場合があるため置換
    return datetime.fromisoformat(date_str.replace('Z', '+00:00'))

def stream_svn_log_entries(file_path: str) -> Iterator[ET.Element]:
    """
    巨大なXMLファイルを省メモリで処理するためのジェネレータ
    logentry要素を一つずつyieldし、処理後にメモリを解放する
    """
    try:
        # events=('end',) で閉じタグのタイミングで処理
        context = ET.iterparse(file_path, events=('end',))
        _, root = next(context)  # ルート要素を取得（最後にクリアするためではないが、context開始のため）

        for event, elem in context:
            if elem.tag == 'logentry':
                yield elem
                # 要素の処理が終わったらメモリから削除
                root.clear() 
    except ET.ParseError as e:
        print(f"XML Parse Error: {e}", file=sys.stderr)
        sys.exit(1)
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.", file=sys.stderr)
        sys.exit(1)

def analyze_svn_log(file_path: str, criteria: FilterCriteria) -> Counter:
    """SVNログを解析してファイル変更数をカウントする"""
    file_counts = Counter()

    for entry in stream_svn_log_entries(file_path):
        # 1. 日付情報の取得とフィルタリング
        date_node = entry.find('date')
        if date_node is not None and date_node.text:
            try:
                entry_date = parse_iso_date(date_node.text)
                if not criteria.is_date_in_range(entry_date):
                    continue
            except ValueError:
                continue  # 日付形式不正の場合はスキップ

        # 2. パス情報の取得とフィルタリング
        # paths要素の下にあるpath要素を取得
        for path_node in entry.findall('.//path'):
            svn_path = path_node.text
            if criteria.is_path_allowed(svn_path):
                file_counts[svn_path] += 1

    return file_counts

def validate_date_arg(s: str) -> datetime:
    """argparse用の日付検証関数"""
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: '{s}'. Use YYYY-MM-DD.")

def print_report(counts: Counter, args: argparse.Namespace):
    """分析結果の表示"""
    print("=" * 70)
    print(f"SVN Log Analysis Report")
    print("=" * 70)
    
    # フィルタ条件の表示
    if args.start:
        print(f"Start Date: {args.start.date()}")
    if args.end:
        print(f"End Date:   {args.end.date()}")
    if args.ext:
        print(f"Extensions: {', '.join(args.ext)}")
    if args.exclude:
        print(f"Excluded:   {', '.join(args.exclude)}")
    print("-" * 70)

    if not counts:
        print("No matching files found.")
        return

    # ヘッダーとデータ出力
    print(f"{'Count':<8} | File Path")
    print("-" * 70)
    
    # top_n の件数だけ表示
    for path, count in counts.most_common(args.limit):
        print(f"{count:<8} | {path}")

def main():
    parser = argparse.ArgumentParser(description="Advanced SVN XML Log Analyzer")
    
    parser.add_argument("input_file", type=str, help="Path to the SVN XML log file")
    parser.add_argument("-n", "--limit", type=int, default=50, help="Max entries to display")
    parser.add_argument("-e", "--exclude", nargs='+', default=[], help="Exclude dirs (e.g. /tags)")
    parser.add_argument("-ext", "--extension", nargs='+', dest="ext", default=[], help="Filter extensions (e.g. .py)")
    parser.add_argument("-s", "--start", type=validate_date_arg, help="Start date (YYYY-MM-DD)")
    parser.add_argument("-d", "--end", type=validate_date_arg, help="End date (YYYY-MM-DD)")

    args = parser.parse_args()

    # フィルタ条件の構築
    criteria = FilterCriteria(
        exclude_dirs=set(args.exclude),
        extensions=set(args.ext),
        start_date=args.start,
        end_date=args.end
    )

    try:
        counts = analyze_svn_log(args.input_file, criteria)
        print_report(counts, args)
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        sys.exit(130)

if __name__ == "__main__":
    main()