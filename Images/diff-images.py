from pathlib import Path
import csv
import argparse

from PIL import Image
from pixelmatch import pixelmatch

# ==========================================
# 設定
# ==========================================
BASE_DIR = Path(__file__).resolve().parent
THRESHOLD = 0.1
INCLUDE_AA = False


def compare_images(before_path, after_path, diff_path):
    """2つのPNG画像を比較し、一致率を返す。"""

    with Image.open(before_path) as before_img, \
         Image.open(after_path) as after_img:

        if before_img.size != after_img.size:
            return "サイズ不一致", None, None

        width, height = before_img.size

    # pixelmatch-fastによる比較
    diff_count = pixelmatch(
        before_path,
        after_path,
        output=diff_path,
        threshold=THRESHOLD,
        includeAA=INCLUDE_AA,
    )

    total_pixels = width * height
    match_rate = (1 - diff_count / total_pixels) * 100

    status = "一致" if diff_count == 0 else "差分あり"

    return status, diff_count, match_rate


def parse_args():
    parser = argparse.ArgumentParser(
        description="before/after のPNG画像を比較して差分画像とCSVを出力します。"
    )
    parser.add_argument(
        "--before-dir",
        type=Path,
        default=BASE_DIR / "before",
        help="比較元画像フォルダ (default: ./before)",
    )
    parser.add_argument(
        "--after-dir",
        type=Path,
        default=BASE_DIR / "after",
        help="比較先画像フォルダ (default: ./after)",
    )
    parser.add_argument(
        "--result-dir",
        type=Path,
        default=BASE_DIR / "result",
        help="結果出力フォルダ (default: ./result)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    before_dir = args.before_dir
    after_dir = args.after_dir
    result_dir = args.result_dir

    result_dir.mkdir(parents=True, exist_ok=True)

    before_files = {
        p.name: p
        for p in before_dir.glob("*.png")
        if p.is_file()
    }

    after_files = {
        p.name: p
        for p in after_dir.glob("*.png")
        if p.is_file()
    }

    # 両方のフォルダにあるファイル名を対象とする
    all_names = sorted(set(before_files) | set(after_files))

    results = []

    for filename in all_names:
        before_path = before_files.get(filename)
        after_path = after_files.get(filename)

        if before_path is None:
            status, diff_count, match_rate = (
                "移行元なし", None, None
            )

        elif after_path is None:
            status, diff_count, match_rate = (
                "移行先なし", None, None
            )

        else:
            diff_path = result_dir / f"{Path(filename).stem}_diff.png"

            try:
                status, diff_count, match_rate = compare_images(
                    before_path,
                    after_path,
                    diff_path,
                )
            except Exception as e:
                status = f"エラー: {e}"
                diff_count = None
                match_rate = None

        results.append([
            filename,
            status,
            diff_count if diff_count is not None else "",
            f"{match_rate:.4f}" if match_rate is not None else "",
        ])

        rate_text = (
            f"{match_rate:.4f}%"
            if match_rate is not None else "-"
        )

        print(f"{filename}: {status} / 一致率 {rate_text}")

    # CSV出力
    csv_path = result_dir / "summary.csv"

    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "ファイル名",
            "判定",
            "差分ピクセル数",
            "一致率(%)",
        ])
        writer.writerows(results)

    print(f"\n比較完了: {csv_path}")


if __name__ == "__main__":
    main()
