#!/usr/bin/env python3
"""
仕様:
 - コマンドラインで対象フォルダを指定
 - サブディレクトリは検索しない（直下のみ）
 - 指定フォルダにある files.yml を読み込む（無ければエラー終了）
 - files.yml の 'extensions' で削除対象拡張子を複数指定（必須）
 - files.yml の 'keep' はワイルドカード（fnmatch）で除外指定
 - --dry-run で削除予定ファイルを表示する
"""

from __future__ import annotations

import argparse
import fnmatch
import logging
from pathlib import Path
from typing import Dict, List, Set, Iterable
import sys

try:
    import yaml
except Exception as e:
    print("このスクリプトは PyYAML が必要です: pip install pyyaml", file=sys.stderr)
    raise

EXIT_OK = 0
EXIT_INVALID_ARGS = 1
EXIT_NOT_DIR = 2
EXIT_CONFIG_MISSING = 3
EXIT_CONFIG_INVALID = 4
EXIT_DELETE_ERROR = 5

logger = logging.getLogger("remove_unused_files")

def load_config(config_path: Path) -> Dict[str, List[str]]:
    if not config_path.exists():
        logger.error("config file not found: %s", config_path)
        raise FileNotFoundError(f"files.yml not found at: {config_path}")

    with config_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # Validate extensions
    extensions = cfg.get("extensions")
    if not isinstance(extensions, list) or len(extensions) == 0:
        logger.error("'extensions' must be a non-empty list in files.yml")
        raise ValueError("'extensions' must be a non-empty list in files.yml")

    # Normalize extensions: strip leading dot, lowercase
    norm_exts: List[str] = []
    for e in extensions:
        if not isinstance(e, str):
            raise ValueError("each extension must be a string")
        s = e.strip().lstrip(".").lower()
        if s == "":
            raise ValueError("invalid extension in files.yml")
        norm_exts.append(s)

    keep = cfg.get("keep", []) or []
    if not isinstance(keep, list):
        raise ValueError("'keep' must be a list if present")
    keep_patterns = [str(p) for p in keep]

    return {"extensions": norm_exts, "keep": keep_patterns}

def list_files_in_dir(target_dir: Path) -> Iterable[Path]:
    # Only list immediate children; do not recurse.
    for child in sorted(target_dir.iterdir()):
        if child.is_file():
            yield child

def matches_keep(name: str, keep_patterns: Iterable[str]) -> bool:
    for p in keep_patterns:
        if fnmatch.fnmatch(name, p):
            logger.debug("keep pattern matched: %s -> %s", name, p)
            return True
    return False

def extension_allowed(name: str, allowed_exts: Set[str]) -> bool:
    # Use Path.suffix to get last suffix (including leading dot)
    ext = Path(name).suffix.lstrip(".").lower()
    if ext == "":
        return False
    return ext in allowed_exts

def filter_files_to_delete(files: Iterable[Path], allowed_exts: Set[str], keep_patterns: List[str], config_filename: str = "files.yml") -> List[Path]:
    to_delete: List[Path] = []
    for p in files:
        # never delete config file itself
        if p.name == config_filename:
            logger.debug("skip config file: %s", p)
            continue
        if not extension_allowed(p.name, allowed_exts):
            logger.debug("skip by extension: %s", p)
            continue
        if matches_keep(p.name, keep_patterns):
            logger.debug("skip by keep pattern: %s", p)
            continue
        to_delete.append(p)
    return to_delete

def delete_files(files: Iterable[Path], dry_run: bool = True) -> int:
    deleted = 0
    failed = 0
    for p in files:
        if dry_run:
            logger.info("[DRY-RUN] %s", p)
            deleted += 1
            continue
        try:
            p.unlink()
            logger.info("deleted: %s", p)
            deleted += 1
        except Exception as e:
            logger.error("failed to delete %s: %s", p, e)
            failed += 1
    return 0 if failed == 0 else 1

def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="指定フォルダ直下の不要ファイルを削除（サブディレクトリは無視）")
    parser.add_argument("target_dir", help="対象ディレクトリパス")
    parser.add_argument("--dry-run", action="store_true", help="削除を実行せず、削除予定ファイルを出力する")
    parser.add_argument("--verbose", "-v", action="store_true", help="詳細ログを出力する")
    return parser.parse_args(argv)

def main(argv: List[str] | None = None) -> int:
    args = parse_args(argv)

    # logging
    handler = logging.StreamHandler()
    fmt = "%(levelname)s: %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    target = Path(args.target_dir)
    if not target.exists() or not target.is_dir():
        logger.error("指定されたパスがディレクトリではありません: %s", target)
        return EXIT_NOT_DIR

    config_path = Path(__file__).resolve().parent / 'files.yml'
    if not config_path.is_file():
        logger.error("指定ディレクトリに files.yml が見つかりません: %s", config_path)
        return EXIT_CONFIG_MISSING

    try:
        cfg = load_config(config_path)
    except FileNotFoundError:
        return EXIT_CONFIG_MISSING
    except ValueError as e:
        logger.error("config validation error: %s", e)
        return EXIT_CONFIG_INVALID
    except Exception as e:
        logger.error("failed to load config: %s", e)
        return EXIT_CONFIG_INVALID

    allowed_exts: Set[str] = set(cfg["extensions"])
    keep_patterns: List[str] = cfg["keep"]

    files = list_files_in_dir(target)
    to_delete = filter_files_to_delete(files, allowed_exts, keep_patterns, config_filename=config_path.name)

    if not to_delete:
        logger.info("削除対象のファイルはありません。")
        return EXIT_OK

    if args.dry_run:
        logger.info("DRY RUN: 以下が削除対象になります（実際には削除されません）:")
        for p in to_delete:
            logger.info("  %s", p)
        return EXIT_OK

    # 実行モード: 削除
    logger.info("削除を開始します: %d 件", len(to_delete))
    result = delete_files(to_delete, dry_run=False)
    if result != 0:
        return EXIT_DELETE_ERROR

    logger.info("完了: %d 件削除しました。", len(to_delete))
    return EXIT_OK

if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        logger.error("ユーザによる中断")
        sys.exit(EXIT_INVALID_ARGS)