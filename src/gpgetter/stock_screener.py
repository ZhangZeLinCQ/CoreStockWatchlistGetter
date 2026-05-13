#!/usr/bin/env python3
"""A-share stock screener.

Screen Shanghai/Shenzhen main-board stocks by:
1. non-ST / non-*ST
2. aggregate institutional holder count greater than a threshold
3. limit-up count in the selected lookback window greater than a threshold

Data source: Eastmoney public endpoints. The institutional holding date is the
latest reporting period exposed by Eastmoney's JGCC page. Daily K-line data is
fetched from Tencent's quote endpoint.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import html
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import requests


EASTMONEY_JGCC_DATE_URL = "https://datapc.eastmoney.com/emdatacenter/jgcc/getdatelist2"
EASTMONEY_JGCC_LIST_URL = "https://datapc.eastmoney.com/emdatacenter/jgcc/list2"
EASTMONEY_STOCK_INFO_URL = "https://push2.eastmoney.com/api/qt/stock/get"
EASTMONEY_STOCK_INFO_FALLBACK_URL = "https://82.push2.eastmoney.com/api/qt/stock/get"
TENCENT_KLINE_URL = "https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q="
EASTMONEY_QUOTE_UT = "fa5fd1943c7b386f172d6893dbfba10b"

MAIN_BOARD_PREFIXES = ("600", "601", "603", "605", "000", "001", "002", "003")
DEFAULT_LOOKBACK_DAYS = 365
DEFAULT_MIN_LIMIT_UPS = 5
DEFAULT_MIN_INSTITUTIONS = 30
DEFAULT_WORKERS = 6
DEFAULT_BACKGROUND_INTERVAL_DAYS = 1
OUTPUT_DIR = Path("output")
LATEST_OUTPUT_DIR = OUTPUT_DIR / "latest"
LATEST_MARKDOWN_PATH = LATEST_OUTPUT_DIR / "candidates.md"
LATEST_ANALYSIS_MARKDOWN_PATH = LATEST_OUTPUT_DIR / "changes.md"
LATEST_HTML_PATH = LATEST_OUTPUT_DIR / "index.html"
LATEST_ANALYSIS_HTML_PATH = LATEST_OUTPUT_DIR / "changes.html"
SNAPSHOT_FILENAME_RE = re.compile(
    r"screened_stocks_(\d{8})_d(\d+)_lu(\d+)_inst(\d+)\.csv$"
)
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)


@dataclass(frozen=True)
class Candidate:
    code: str
    name: str
    secucode: str
    institution_count: int
    institution_shares: float | None
    institution_ratio: float | None
    holding_report_date: str


@dataclass(frozen=True)
class ScreenedStock:
    code: str
    name: str
    industry: str
    sector: str
    concepts: str
    market: str
    turnover_amount_100m: float | None
    institution_count: int
    institution_shares_10k: float | None
    institution_ratio_pct: float | None
    limit_up_count: int
    holding_report_date: str
    price_start_date: str
    price_end_date: str


@dataclass(frozen=True)
class StockChange:
    change_type: str
    code: str
    name: str
    current_turnover_amount_100m: float | None
    previous_turnover_amount_100m: float | None
    turnover_amount_delta_100m: float | None
    current_institution_count: int | None
    previous_institution_count: int | None
    institution_count_delta: int | None
    current_limit_up_count: int | None
    previous_limit_up_count: int | None
    current_holding_report_date: str
    previous_holding_report_date: str


@dataclass(frozen=True)
class AnalysisReport:
    generated_at: str
    current_path: Path
    previous_path: Path | None
    lookback_days: int
    min_limit_ups: int
    min_institutions: int
    current_count: int
    previous_count: int
    added: list[StockChange]
    removed: list[StockChange]
    institution_increased: list[StockChange]
    institution_decreased: list[StockChange]
    note: str = ""


@dataclass(frozen=True)
class RecentWindowChanges:
    days: int
    current_path: Path
    baseline_path: Path | None
    current_date: dt.date | None
    baseline_date: dt.date | None
    added: list[StockChange]
    removed: list[StockChange]
    note: str = ""


@dataclass(frozen=True)
class StockHistoryPoint:
    snapshot_date: dt.date
    snapshot_path: Path
    turnover_amount_100m: float | None
    institution_count: int
    limit_up_count: int


def parse_args() -> argparse.Namespace:
    today = dt.date.today()

    parser = argparse.ArgumentParser(
        description=(
            "筛选沪深主板：近 X 天涨停次数 > N、非 ST、机构汇总持股家数 > M，"
            "并导出 CSV 列表。"
        )
    )
    parser.add_argument(
        "--min-limit-ups",
        type=int,
        default=None,
        help=f"涨停次数阈值，默认 {DEFAULT_MIN_LIMIT_UPS}",
    )
    parser.add_argument(
        "--min-institutions",
        type=int,
        default=None,
        help=f"机构汇总持股家数阈值，默认 {DEFAULT_MIN_INSTITUTIONS}",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="价格统计结束日期，格式 YYYYMMDD，默认今天",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help=f"价格统计回看自然日天数，默认 {DEFAULT_LOOKBACK_DAYS}",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"并发拉取日线的线程数，默认 {DEFAULT_WORKERS}",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="输出 CSV 文件路径，默认 output/screened_stocks_日期_筛选参数.csv",
    )
    parser.add_argument(
        "--force-update",
        action="store_true",
        help="即使当天同口径结果已存在，也重新拉取并覆盖输出文件",
    )
    parser.add_argument(
        "--skip-analysis",
        action="store_true",
        help="只更新筛选结果，不生成历史变化分析",
    )
    parser.add_argument(
        "--skip-html",
        action="store_true",
        help="不生成 HTML 网页",
    )
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help="只读取现有筛选结果并生成历史变化分析，不拉取新数据",
    )
    parser.add_argument(
        "--analysis-output",
        default=None,
        help="分析 Markdown 文件路径，默认 output/analysis_日期_筛选参数.md",
    )
    parser.add_argument(
        "--analysis-previous",
        default=None,
        help="指定用于对比的历史 CSV；默认自动选择上一份同口径快照",
    )
    parser.add_argument(
        "--history-dir",
        default=str(OUTPUT_DIR),
        help="自动查找历史 CSV 的目录，默认 output",
    )
    parser.add_argument(
        "--background",
        action="store_true",
        help=f"后台循环运行，每 {DEFAULT_BACKGROUND_INTERVAL_DAYS} 天执行一次",
    )
    parser.add_argument(
        "--background-interval-days",
        type=int,
        default=DEFAULT_BACKGROUND_INTERVAL_DAYS,
        help=f"后台运行间隔自然日天数，默认 {DEFAULT_BACKGROUND_INTERVAL_DAYS}",
    )
    parser.add_argument(
        "--backfill-days",
        type=int,
        default=None,
        help="批量回填截至结束日期的最近 N 个自然日快照，例如 30 表示过去 30 天每天一份",
    )
    args = parser.parse_args()
    args.end_date_was_provided = args.end_date is not None
    args.output_was_provided = args.output is not None
    args.analysis_output_was_provided = args.analysis_output is not None
    if args.analyze_only and args.skip_analysis:
        parser.error("--analyze-only 不能和 --skip-analysis 同时使用")
    resolve_screening_inputs(args, today)
    validate_backfill_args(parser, args)
    return args


def resolve_screening_inputs(args: argparse.Namespace, today: dt.date) -> None:
    interactive = sys.stdin.isatty()
    args.lookback_days = resolve_positive_int(
        current_value=args.lookback_days,
        default_value=DEFAULT_LOOKBACK_DAYS,
        prompt="请输入统计天数",
        interactive=interactive,
    )
    args.min_limit_ups = resolve_positive_int(
        current_value=args.min_limit_ups,
        default_value=DEFAULT_MIN_LIMIT_UPS,
        prompt="请输入涨停次数大于多少",
        interactive=interactive,
        allow_zero=True,
    )
    args.min_institutions = resolve_positive_int(
        current_value=args.min_institutions,
        default_value=DEFAULT_MIN_INSTITUTIONS,
        prompt="请输入机构持股家数大于多少",
        interactive=interactive,
        allow_zero=True,
    )
    args.background_interval_days = validate_positive_int(
        args.background_interval_days,
        "后台运行间隔天数",
    )
    apply_default_dates_and_paths(args, today)


def validate_backfill_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.backfill_days is None:
        return
    try:
        args.backfill_days = validate_positive_int(args.backfill_days, "回填天数")
    except ValueError as exc:
        parser.error(str(exc))
    if args.background:
        parser.error("--backfill-days 不能和 --background 同时使用")
    if args.output_was_provided:
        parser.error("--backfill-days 会按日期生成多个文件，不能同时指定 --output")
    if args.analysis_output_was_provided:
        parser.error("--backfill-days 会按日期生成多个分析文件，不能同时指定 --analysis-output")
    if args.analysis_previous:
        parser.error("--backfill-days 会逐日自动选择上一份快照，不能同时指定 --analysis-previous")


def apply_default_dates_and_paths(args: argparse.Namespace, today: dt.date) -> None:
    if not args.end_date_was_provided:
        args.end_date = f"{today:%Y%m%d}"

    output_date = parse_trade_date(args.end_date)
    if not args.output_was_provided:
        args.output = str(default_screening_output_path(args, output_date))
    if not args.analysis_output_was_provided:
        args.analysis_output = str(default_analysis_output_path(args, output_date))


def default_screening_output_path(args: argparse.Namespace, output_date: dt.date) -> Path:
    return OUTPUT_DIR / (
        f"screened_stocks_{output_date:%Y%m%d}"
        f"_d{args.lookback_days}"
        f"_lu{args.min_limit_ups}"
        f"_inst{args.min_institutions}.csv"
    )


def default_analysis_output_path(args: argparse.Namespace, output_date: dt.date) -> Path:
    return OUTPUT_DIR / (
        f"analysis_{output_date:%Y%m%d}"
        f"_d{args.lookback_days}"
        f"_lu{args.min_limit_ups}"
        f"_inst{args.min_institutions}.md"
    )


def resolve_positive_int(
    current_value: int | None,
    default_value: int,
    prompt: str,
    interactive: bool,
    allow_zero: bool = False,
) -> int:
    if current_value is not None:
        return validate_positive_int(current_value, prompt, allow_zero)
    if not interactive:
        return default_value

    while True:
        raw_value = input(f"{prompt} [{default_value}]: ").strip()
        if not raw_value:
            return default_value
        try:
            return validate_positive_int(int(raw_value), prompt, allow_zero)
        except ValueError as exc:
            print(exc)


def validate_positive_int(value: int, label: str, allow_zero: bool = False) -> int:
    min_value = 0 if allow_zero else 1
    if value < min_value:
        raise ValueError(f"{label}必须大于等于 {min_value}。")
    return value


def request_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any] | None = None,
    retries: int = 3,
    timeout: int = 20,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:  # requests can raise several transport/json errors
            last_error = exc
            if attempt < retries - 1:
                time.sleep(0.8 * (attempt + 1))
    raise RuntimeError(f"请求失败: {url}; {last_error}") from last_error


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://datapc.eastmoney.com/",
        }
    )
    return session


def latest_holding_report_date(session: requests.Session) -> str:
    payload = request_json(session, EASTMONEY_JGCC_DATE_URL)
    data = payload.get("result", {}).get("data", [])
    if not data:
        raise RuntimeError("未能获取机构持仓报告期。")
    return str(data[0]["REPORT_DATE"]).split(" ")[0]


def is_main_board_code(code: str) -> bool:
    return code.startswith(MAIN_BOARD_PREFIXES)


def is_non_st_name(name: str) -> bool:
    upper_name = name.upper()
    return "ST" not in upper_name


def market_name(code: str) -> str:
    if code.startswith("6"):
        return "上海主板"
    return "深圳主板"


def tencent_symbol_for_code(code: str) -> str:
    prefix = "sh" if code.startswith("6") else "sz"
    return f"{prefix}{code}"


def eastmoney_secid_for_code(code: str) -> str:
    market_id = "1" if code.startswith("6") else "0"
    return f"{market_id}.{code}"


def fetch_stock_metadata(code: str) -> tuple[str, str, str]:
    session = create_session()
    session.headers.update({"Referer": "https://quote.eastmoney.com/"})
    params = {
        "secid": eastmoney_secid_for_code(code),
        "ut": EASTMONEY_QUOTE_UT,
        "fields": "f57,f58,f127,f128,f129",
    }
    errors: list[str] = []
    for url in (EASTMONEY_STOCK_INFO_FALLBACK_URL, EASTMONEY_STOCK_INFO_URL):
        try:
            payload = request_json(session, url, params=params, retries=5, timeout=10)
            data = payload.get("data") or {}
            return (
                str(data.get("f127") or ""),
                str(data.get("f128") or ""),
                str(data.get("f129") or ""),
            )
        except Exception as exc:
            errors.append(str(exc))
            time.sleep(0.3)

    print(f"[WARN] {code} 行业/板块/概念获取失败: {'; '.join(errors)}", file=sys.stderr)
    return "", "", ""


def fetch_turnover_amount_100m(code: str) -> float | None:
    symbol = tencent_symbol_for_code(code)
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Referer": "https://gu.qq.com/"})

    try:
        response = session.get(f"{TENCENT_QUOTE_URL}{symbol}", timeout=10)
        response.raise_for_status()
    except Exception as exc:
        print(f"[WARN] {code} 资金量获取失败: {exc}", file=sys.stderr)
        return None

    try:
        payload = response.text.split("=", 1)[1].strip().strip(";").strip('"')
        parts = payload.split("~")
        amount_text = parts[35].split("/")[2]
        return float(amount_text) / 100000000
    except Exception as exc:
        print(f"[WARN] {code} 资金量解析失败: {exc}", file=sys.stderr)
        return None


def fetch_institution_candidates(
    session: requests.Session,
    report_date: str,
    min_institutions: int,
) -> list[Candidate]:
    candidates: list[Candidate] = []
    page = 1
    page_size = 500

    while True:
        params = {
            "stat": 0,
            "st": "HOULD_NUM",
            "sr": -1,
            "p": page,
            "ps": page_size,
            "cmd": 0,
            "fd": report_date,
        }
        payload = request_json(session, EASTMONEY_JGCC_LIST_URL, params=params)
        result = payload.get("result", {})
        rows = result.get("data", [])
        if not rows:
            break

        for row in rows:
            if row.get("ORG_TYPE_NAME") != "机构汇总" and row.get("ORG_TYPE") != "00":
                continue

            code = str(row.get("SECURITY_CODE", "")).strip()
            name = str(row.get("SECURITY_NAME_ABBR", "")).strip()
            institution_count = int(row.get("HOULD_NUM") or 0)

            if institution_count <= min_institutions:
                continue
            if not code or not is_main_board_code(code) or not is_non_st_name(name):
                continue

            candidates.append(
                Candidate(
                    code=code,
                    name=name,
                    secucode=str(row.get("SECUCODE", "")).strip(),
                    institution_count=institution_count,
                    institution_shares=to_float(row.get("TOTAL_SHARES")),
                    institution_ratio=to_float(row.get("TOTALSHARES_RATIO")),
                    holding_report_date=report_date,
                )
            )

        total_pages = int(result.get("pages") or 0)
        if total_pages and page >= total_pages:
            break
        page += 1

    # Some Eastmoney pages include repeated rows around institution categories.
    deduped: dict[str, Candidate] = {}
    for item in candidates:
        current = deduped.get(item.code)
        if current is None or item.institution_count > current.institution_count:
            deduped[item.code] = item
    return sorted(deduped.values(), key=lambda item: item.institution_count, reverse=True)


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_trade_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y%m%d").date()


def fetch_limit_up_count(
    candidate: Candidate,
    begin_date: dt.date,
    end_date: dt.date,
    min_limit_up_pct: float = 9.8,
) -> tuple[Candidate, int, str, str, float | None]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Referer": "https://gu.qq.com/"})
    symbol = tencent_symbol_for_code(candidate.code)
    query_begin_date = begin_date - dt.timedelta(days=14)
    params = {
        "param": (
            f"{symbol},day,{query_begin_date:%Y-%m-%d},{end_date:%Y-%m-%d},"
            f"{max(400, (end_date - query_begin_date).days + 30)},qfq"
        )
    }
    payload = request_json(session, TENCENT_KLINE_URL, params=params, timeout=15)
    kline_data = payload.get("data", {}).get(symbol) or {}
    klines = kline_data.get("qfqday") or kline_data.get("day") or []

    limit_up_count = 0
    first_date = ""
    last_date = ""
    turnover_amount_100m: float | None = None
    previous_close: float | None = None
    for row in klines:
        if len(row) < 5:
            continue

        trade_date = dt.date.fromisoformat(str(row[0]))
        close_price = to_float(row[2])
        high_price = to_float(row[3])
        if close_price is None:
            continue

        if trade_date < begin_date:
            previous_close = close_price
            continue
        if trade_date > end_date:
            break

        if not first_date:
            first_date = f"{trade_date:%Y-%m-%d}"
        last_date = f"{trade_date:%Y-%m-%d}"
        turnover_amount_100m = estimate_turnover_amount_from_kline_100m(row, close_price)

        # For non-ST Shanghai/Shenzhen main-board stocks, normal daily limit is 10%.
        # Requiring close == high reduces false positives from intraday spikes.
        pct_change = (
            (close_price / previous_close - 1) * 100
            if previous_close not in (None, 0)
            else None
        )
        if (
            pct_change is not None
            and high_price is not None
            and pct_change >= min_limit_up_pct
            and abs(close_price - high_price) < 0.001
        ):
            limit_up_count += 1
        previous_close = close_price

    return candidate, limit_up_count, first_date, last_date, turnover_amount_100m


def estimate_turnover_amount_from_kline_100m(row: list[Any], close_price: float) -> float | None:
    if len(row) < 6:
        return None
    volume_lots = to_float(row[5])
    if volume_lots is None:
        return None
    return volume_lots * 100 * close_price / 100000000


def screen_stocks(args: argparse.Namespace) -> list[ScreenedStock]:
    end_date = parse_trade_date(args.end_date)
    begin_date = end_date - dt.timedelta(days=args.lookback_days)

    session = create_session()
    report_date = latest_holding_report_date(session)
    candidates = fetch_institution_candidates(session, report_date, args.min_institutions)

    if not candidates:
        return []

    results: list[ScreenedStock] = []
    failed_candidates: list[Candidate] = []
    total = len(candidates)
    done = 0
    use_historical_turnover = bool(getattr(args, "use_historical_turnover", False))

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
        futures = {
            executor.submit(fetch_limit_up_count, candidate, begin_date, end_date): candidate
            for candidate in candidates
        }
        for future in as_completed(futures):
            done += 1
            candidate = futures[future]
            try:
                item, limit_up_count, first_date, last_date, turnover_amount_100m = future.result()
            except Exception as exc:
                failed_candidates.append(candidate)
                continue

            append_result_if_matched(
                results,
                item,
                limit_up_count,
                first_date,
                last_date,
                turnover_amount_100m if use_historical_turnover else None,
                args.min_limit_ups,
            )

            if done % 50 == 0 or done == total:
                print(f"已检查 {done}/{total} 只候选股票...", file=sys.stderr)

    if failed_candidates:
        print(f"正在补拉 {len(failed_candidates)} 只首次失败的股票...", file=sys.stderr)
        for candidate in failed_candidates:
            try:
                (
                    item,
                    limit_up_count,
                    first_date,
                    last_date,
                    turnover_amount_100m,
                ) = fetch_limit_up_count(candidate, begin_date, end_date)
            except Exception as exc:
                print(f"[WARN] {candidate.code} {candidate.name} 日线获取失败: {exc}", file=sys.stderr)
                continue
            append_result_if_matched(
                results,
                item,
                limit_up_count,
                first_date,
                last_date,
                turnover_amount_100m if use_historical_turnover else None,
                args.min_limit_ups,
            )
            time.sleep(0.2)

    sorted_results = sorted(
        results,
        key=lambda item: (item.limit_up_count, item.institution_count),
        reverse=True,
    )
    return enrich_stock_metadata(sorted_results)


def enrich_stock_metadata(rows: list[ScreenedStock]) -> list[ScreenedStock]:
    enriched_rows: list[ScreenedStock] = []
    for row in rows:
        industry, sector, concepts = fetch_stock_metadata(row.code)
        turnover_amount_100m = row.turnover_amount_100m
        if turnover_amount_100m is None:
            turnover_amount_100m = fetch_turnover_amount_100m(row.code)
        enriched_rows.append(
            replace(
                row,
                industry=industry,
                sector=sector,
                concepts=concepts,
                turnover_amount_100m=turnover_amount_100m,
            )
        )
        time.sleep(0.15)
    return enriched_rows


def append_result_if_matched(
    results: list[ScreenedStock],
    item: Candidate,
    limit_up_count: int,
    first_date: str,
    last_date: str,
    turnover_amount_100m: float | None,
    min_limit_ups: int,
) -> None:
    if limit_up_count <= min_limit_ups:
        return

    results.append(
        ScreenedStock(
            code=item.code,
            name=item.name,
            industry="",
            sector="",
            concepts="",
            market=market_name(item.code),
            turnover_amount_100m=turnover_amount_100m,
            institution_count=item.institution_count,
            institution_shares_10k=(
                item.institution_shares / 10000 if item.institution_shares is not None else None
            ),
            institution_ratio_pct=item.institution_ratio,
            limit_up_count=limit_up_count,
            holding_report_date=item.holding_report_date,
            price_start_date=first_date,
            price_end_date=last_date,
        )
    )


def output_fieldnames() -> list[str]:
    return [
        "股票代码",
        "股票名称",
        "所属行业",
        "相关概念",
        "资金量(亿元)",
        "涨停次数",
        "机构数",
        "机构持股总数(万股)",
        "机构持股占总股本比例(%)",
        "机构持仓报告期",
        "价格统计开始日",
        "价格统计结束日",
    ]


def row_to_output_dict(row: ScreenedStock) -> dict[str, str | int]:
    return {
        "股票代码": row.code,
        "股票名称": row.name,
        "所属行业": row.industry,
        "相关概念": row.concepts,
        "资金量(亿元)": format_number(row.turnover_amount_100m),
        "涨停次数": row.limit_up_count,
        "机构数": row.institution_count,
        "机构持股总数(万股)": format_number(row.institution_shares_10k),
        "机构持股占总股本比例(%)": format_number(row.institution_ratio_pct),
        "机构持仓报告期": row.holding_report_date,
        "价格统计开始日": row.price_start_date,
        "价格统计结束日": row.price_end_date,
    }


def write_csv(path: Path, rows: list[ScreenedStock]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=output_fieldnames())
        writer.writeheader()
        for row in rows:
            writer.writerow(row_to_output_dict(row))


def write_markdown(path: Path, rows: list[ScreenedStock]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = output_fieldnames()
    lines = [
        "| " + " | ".join(fieldnames) + " |",
        "| " + " | ".join(["---"] * len(fieldnames)) + " |",
    ]
    for row in rows:
        record = row_to_output_dict(row)
        lines.append("| " + " | ".join(markdown_cell(record[name]) for name in fieldnames) + " |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_stock_html(
    path: Path,
    rows: list[ScreenedStock],
    args: argparse.Namespace,
    source_path: Path,
    recent_changes: RecentWindowChanges,
    detail_href_prefix: str,
) -> None:
    generated_at = f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}"
    records: list[dict[str, object]] = []
    for row in rows:
        record: dict[str, object] = row_to_output_dict(row)
        record["查看详情"] = detail_link_html(detail_href_prefix, row)
        record["__concept_tags__"] = concept_tags(row.concepts)
        records.append(record)
    candidate_fieldnames = output_fieldnames() + ["查看详情"]
    cloud_items = build_concept_tag_counts(rows)
    empty_concept_count = sum(1 for row in rows if not concept_tags(row.concepts))
    body = [
        html_header(
            title="机构涨停候选股",
            subtitle=(
                f"最近 {args.lookback_days} 天涨停次数 > {args.min_limit_ups}，"
                f"机构数 > {args.min_institutions}"
            ),
        ),
        render_dashboard_jump_nav(),
        render_metric_cards(
            [
                ("生成时间", generated_at),
                ("候选股票", f"{len(rows)} 只"),
                ("数据文件", str(source_path)),
                ("数据源", "东方财富机构持仓、腾讯日线行情"),
            ]
        ),
        render_recent_window_summary(recent_changes),
        render_concept_tag_cloud(cloud_items, len(rows), empty_concept_count),
        '<section id="candidate-table" class="panel"><h2>全部候选股票</h2>'
        + render_candidate_html_table(candidate_fieldnames, records, raw_fields={"查看详情"})
        + "</section>",
        render_recent_window_section("recent-added", "最近 5 日新增股票", recent_changes.added),
        render_recent_window_section("recent-removed", "最近 5 日消失股票", recent_changes.removed),
        '<p class="disclaimer">仅供研究，不构成投资建议。</p>',
        render_concept_filter_script(),
    ]
    write_html_document(path, "机构涨停候选股", "\n".join(body))


def detail_link_html(detail_href_prefix: str, row: ScreenedStock) -> str:
    href = f"{detail_href_prefix}{row.code}.html"
    label = f"查看 {row.code} {row.name} 详情"
    return (
        f'<a class="detail-link" href="{html_escape(href)}" '
        f'aria-label="{html_escape(label)}">查看详情</a>'
    )


def render_recent_window_summary(changes: RecentWindowChanges) -> str:
    baseline = format_snapshot_label(changes.baseline_path, changes.baseline_date)
    current = format_snapshot_label(changes.current_path, changes.current_date)
    note = f'<div class="note">{html_escape(changes.note)}</div>' if changes.note else ""
    return (
        '<section class="sources dashboard-note">'
        f"<div>近 {changes.days} 日窗口基准: {html_escape(baseline)}</div>"
        f"<div>当前快照: {html_escape(current)}</div>"
        f"<div>新增 {len(changes.added)} 只，消失 {len(changes.removed)} 只。</div>"
        "</section>"
        f"{note}"
    )


def render_dashboard_jump_nav() -> str:
    return (
        '<nav class="jump-nav" aria-label="页面快速导览">'
        f"{render_theme_toggle_button()}"
        '<a href="#candidate-table">总表</a>'
        '<a href="#recent-added">新增</a>'
        '<a href="#recent-removed">消失</a>'
        "</nav>"
    )


def render_recent_window_section(section_id: str, title: str, changes: list[StockChange]) -> str:
    if not changes:
        return (
            f'<section id="{html_escape(section_id)}" class="panel"><h2>{html_escape(title)}</h2>'
            '<div class="empty">无变化</div></section>'
        )
    return (
        f'<section id="{html_escape(section_id)}" class="panel"><h2>{html_escape(title)}</h2>'
        f"{render_html_table(analysis_fieldnames(), [change_to_output_dict(change) for change in changes])}"
        "</section>"
    )


CONCEPT_SPLIT_RE = re.compile(r"[,，、;/|]+")
CONCEPT_NOISE_TERMS = {"", "概念", "相关概念"}
DEFAULT_VISIBLE_CONCEPT_TAGS = 10
EMPTY_CONCEPT_FILTER = "__empty__"
CONCEPT_COLOR_PALETTE = (
    ("#f7d7c4", "#6f2d12"),
    ("#dcebd0", "#315319"),
    ("#d9e7fb", "#214e88"),
    ("#f5dfb7", "#6c4a08"),
    ("#eadcf5", "#583477"),
    ("#d6efef", "#165d5d"),
    ("#f6dce2", "#7d3147"),
    ("#e6e1d5", "#544633"),
)


def concept_tags(value: str) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()
    for raw_part in CONCEPT_SPLIT_RE.split(value):
        tag = raw_part.strip()
        if not tag:
            continue
        tag = tag.replace("概念", "").strip()
        if tag in CONCEPT_NOISE_TERMS or tag in seen:
            continue
        seen.add(tag)
        tags.append(tag)
    return tags


def build_concept_tag_counts(rows: list[ScreenedStock]) -> list[tuple[str, int]]:
    counts: dict[str, int] = {}
    for row in rows:
        for tag in concept_tags(row.concepts):
            counts[tag] = counts.get(tag, 0) + 1
    return sorted(counts.items(), key=lambda item: (-item[1], item[0]))


def concept_style_attr(label: str) -> str:
    palette_index = sum(ord(char) for char in label) % len(CONCEPT_COLOR_PALETTE)
    background, ink = CONCEPT_COLOR_PALETTE[palette_index]
    return f"--concept-bg:{background};--concept-ink:{ink};"


def render_concept_tag_cloud(
    items: list[tuple[str, int]],
    total_rows: int,
    empty_count: int,
) -> str:
    buttons = [
        (
            '<button class="concept-tag concept-tag-empty" type="button" '
            f'data-concept-filter="{EMPTY_CONCEPT_FILTER}" '
            'style="--concept-bg:#e5e7eb;--concept-ink:#4b5563;">'
            f"暂无 <span>{empty_count}</span>"
            "</button>"
        )
    ]
    buttons.append(
        (
            '<button class="concept-tag is-active" type="button" data-concept-filter="">'
            f"全部 <span>{total_rows}</span>"
            "</button>"
        )
    )
    for index, (label, count) in enumerate(items):
        hidden_attr = ' hidden data-extra-concept-tag="true"' if index >= DEFAULT_VISIBLE_CONCEPT_TAGS else ""
        buttons.append(
            '<button class="concept-tag" type="button" '
            f'data-concept-filter="{html_escape(label)}"{hidden_attr} '
            f'style="{concept_style_attr(label)}">'
            f"{html_escape(label)} <span>{count}</span>"
            "</button>"
        )
    toggle = ""
    if len(items) > DEFAULT_VISIBLE_CONCEPT_TAGS:
        toggle = (
            '<button class="concept-toggle" type="button" '
            'data-concept-toggle aria-expanded="false">展开全部</button>'
        )
    return (
        '<section class="panel concept-cloud-panel" aria-label="相关概念标签筛选">'
        "<h2>概念标签</h2>"
        '<div class="concept-toolbar">'
        '<div class="concept-summary" data-concept-summary>'
        f"当前显示 {total_rows} / {total_rows} 只股票"
        "</div>"
        f"{toggle}"
        "</div>"
        '<div class="concept-cloud">'
        + "".join(buttons)
        + "</div></section>"
    )


def render_concept_filter_script() -> str:
    return """
<script>
(() => {
  const buttons = Array.from(document.querySelectorAll("[data-concept-filter]"));
  const rows = Array.from(document.querySelectorAll("#candidate-table tbody tr[data-concepts]"));
  const chips = Array.from(document.querySelectorAll("[data-concept-chip]"));
  const summary = document.querySelector("[data-concept-summary]");
  const toggle = document.querySelector("[data-concept-toggle]");
  const extraButtons = Array.from(document.querySelectorAll("[data-extra-concept-tag]"));
  if (!buttons.length || !rows.length || !summary) return;

  const total = rows.length;
  let expanded = false;
  const selected = new Set();
  const applyFilter = () => {
    let visible = 0;
    rows.forEach((row) => {
      const tags = (row.dataset.concepts || "").split("|").filter(Boolean);
      const matched = !selected.size || Array.from(selected).some((tag) => {
        if (tag === "__empty__") return tags.length === 0;
        return tags.includes(tag);
      });
      row.hidden = !matched;
      if (matched) visible += 1;
    });
    buttons.forEach((button) => {
      const tag = button.dataset.conceptFilter || "";
      button.classList.toggle("is-active", tag ? selected.has(tag) : !selected.size);
    });
    chips.forEach((chip) => {
      chip.classList.toggle("is-selected-match", selected.has(chip.dataset.conceptChip || ""));
    });
    const labels = buttons
      .filter((button) => {
        const tag = button.dataset.conceptFilter || "";
        return tag && selected.has(tag);
      })
      .map((button) => button.dataset.conceptFilter === "__empty__" ? "暂无" : button.textContent.trim().replace(/\\s+\\d+$/, ""));
    summary.textContent = labels.length
      ? `当前标签: ${labels.join("、")}，显示 ${visible} / ${total} 只股票`
      : `当前显示 ${visible} / ${total} 只股票`;
  };

  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      const tag = button.dataset.conceptFilter || "";
      if (!tag) {
        selected.clear();
      } else if (selected.has(tag)) {
        selected.delete(tag);
      } else {
        selected.add(tag);
      }
      applyFilter();
    });
  });

  if (toggle && extraButtons.length) {
    toggle.addEventListener("click", () => {
      expanded = !expanded;
      extraButtons.forEach((button) => {
        button.hidden = !expanded;
      });
      toggle.textContent = expanded ? "收起" : "展开全部";
      toggle.setAttribute("aria-expanded", expanded ? "true" : "false");
    });
  }
})();
</script>
""".strip()


def render_theme_toggle_script() -> str:
    return """
<script>
(() => {
  const root = document.documentElement;
  const buttons = Array.from(document.querySelectorAll("[data-theme-toggle]"));
  const storageKey = "gpgetter-theme";
  const savedTheme = window.localStorage.getItem(storageKey);
  const applyTheme = (theme) => {
    const nextTheme = theme === "dark" ? "dark" : "light";
    root.dataset.theme = nextTheme;
    const nextLabel = nextTheme === "dark" ? "切换到浅色主题" : "切换到深色主题";
    buttons.forEach((button) => {
      button.setAttribute("aria-label", nextLabel);
      button.setAttribute("title", nextLabel);
      button.setAttribute("aria-pressed", nextTheme === "dark" ? "true" : "false");
    });
  };

  applyTheme(savedTheme || "light");
  if (!buttons.length) return;

  buttons.forEach((button) => {
    button.addEventListener("click", () => {
      const nextTheme = root.dataset.theme === "dark" ? "light" : "dark";
      window.localStorage.setItem(storageKey, nextTheme);
      applyTheme(nextTheme);
    });
  });
})();
</script>
""".strip()


def render_sticky_table_header_script() -> str:
    return """
<script>
(() => {
  const wrappers = Array.from(document.querySelectorAll(".table-wrap"));
  if (!wrappers.length) return;

  const sticky = document.createElement("div");
  sticky.className = "sticky-table-header";
  sticky.setAttribute("aria-hidden", "true");
  const stickyTable = document.createElement("table");
  sticky.appendChild(stickyTable);
  document.body.appendChild(sticky);

  let activeWrapper = null;
  const syncHeader = (wrapper) => {
    const table = wrapper.querySelector("table");
    if (!table || !table.tHead) return;
    const wrapRect = wrapper.getBoundingClientRect();
    sticky.style.left = `${wrapRect.left}px`;
    sticky.style.width = `${wrapRect.width}px`;
    stickyTable.style.width = `${table.getBoundingClientRect().width}px`;
    stickyTable.style.transform = `translateX(${-wrapper.scrollLeft}px)`;

    if (activeWrapper !== wrapper) {
      stickyTable.innerHTML = table.tHead.outerHTML;
      activeWrapper = wrapper;
    }

    const sourceCells = Array.from(table.tHead.querySelectorAll("th"));
    const clonedCells = Array.from(stickyTable.querySelectorAll("th"));
    sourceCells.forEach((cell, index) => {
      const width = `${cell.getBoundingClientRect().width}px`;
      if (!clonedCells[index]) return;
      clonedCells[index].style.width = width;
      clonedCells[index].style.minWidth = width;
      clonedCells[index].style.maxWidth = width;
    });
  };

  const updateStickyHeader = () => {
    const visibleWrapper = wrappers.find((wrapper) => {
      const rect = wrapper.getBoundingClientRect();
      return rect.top < 0 && rect.bottom > 42;
    });

    if (!visibleWrapper) {
      sticky.style.display = "none";
      activeWrapper = null;
      return;
    }

    sticky.style.display = "block";
    syncHeader(visibleWrapper);
  };

  wrappers.forEach((wrapper) => {
    wrapper.addEventListener("scroll", updateStickyHeader, { passive: true });
  });
  window.addEventListener("scroll", updateStickyHeader, { passive: true });
  window.addEventListener("resize", updateStickyHeader);
  updateStickyHeader();
})();
</script>
""".strip()


def format_snapshot_label(path: Path | None, snapshot_date: dt.date | None) -> str:
    if path is None:
        return "无"
    date_label = f"{snapshot_date:%Y-%m-%d}" if snapshot_date else "日期未知"
    return f"{date_label} / {path}"


def read_screened_csv(path: Path) -> list[ScreenedStock]:
    rows: list[ScreenedStock] = []
    with path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        for record in reader:
            code = str(record.get("股票代码", "")).strip()
            if not code:
                continue
            rows.append(
                ScreenedStock(
                    code=code,
                    name=str(record.get("股票名称", "")).strip(),
                    industry=str(record.get("所属行业", "")).strip(),
                    sector="",
                    concepts=str(record.get("相关概念", "")).strip(),
                    market=market_name(code),
                    turnover_amount_100m=parse_optional_float(record.get("资金量(亿元)")),
                    institution_count=parse_int(record.get("机构数")),
                    institution_shares_10k=parse_optional_float(record.get("机构持股总数(万股)")),
                    institution_ratio_pct=parse_optional_float(
                        record.get("机构持股占总股本比例(%)")
                    ),
                    limit_up_count=parse_int(record.get("涨停次数")),
                    holding_report_date=str(record.get("机构持仓报告期", "")).strip(),
                    price_start_date=str(record.get("价格统计开始日", "")).strip(),
                    price_end_date=str(record.get("价格统计结束日", "")).strip(),
                )
            )
    return rows


def parse_int(value: Any) -> int:
    if value is None:
        return 0
    text = str(value).strip().replace(",", "")
    if not text:
        return 0
    return int(float(text))


def parse_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def build_analysis_report(
    args: argparse.Namespace,
    current_path: Path,
    current_rows: list[ScreenedStock],
) -> AnalysisReport:
    previous_path = resolve_previous_snapshot_path(args, current_path)
    generated_at = f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}"

    if previous_path is None:
        return AnalysisReport(
            generated_at=generated_at,
            current_path=current_path,
            previous_path=None,
            lookback_days=args.lookback_days,
            min_limit_ups=args.min_limit_ups,
            min_institutions=args.min_institutions,
            current_count=len(current_rows),
            previous_count=0,
            added=[],
            removed=[],
            institution_increased=[],
            institution_decreased=[],
            note=(
                "未找到上一份同口径历史快照；本次只保存当前结果，"
                "下次同口径运行后会生成新增、删除和机构数变化。"
            ),
        )

    previous_rows = read_screened_csv(previous_path)
    return compare_snapshots(args, current_path, current_rows, previous_path, previous_rows, generated_at)


def resolve_previous_snapshot_path(args: argparse.Namespace, current_path: Path) -> Path | None:
    if args.analysis_previous:
        previous_path = Path(args.analysis_previous)
        if not previous_path.exists():
            raise FileNotFoundError(f"指定的历史 CSV 不存在: {previous_path}")
        return previous_path

    history_dir = Path(args.history_dir)
    if not history_dir.exists():
        return None

    current_resolved = current_path.resolve()
    current_snapshot_date = parse_snapshot_date(current_path)
    pattern = (
        f"screened_stocks_*"
        f"_d{args.lookback_days}"
        f"_lu{args.min_limit_ups}"
        f"_inst{args.min_institutions}.csv"
    )
    candidates: list[Path] = []
    for path in history_dir.glob(pattern):
        if not path.is_file():
            continue
        if path.resolve() == current_resolved:
            continue
        snapshot_date = parse_snapshot_date(path)
        if current_snapshot_date is not None and snapshot_date is not None:
            if snapshot_date >= current_snapshot_date:
                continue
        candidates.append(path)

    if not candidates:
        return None
    return max(candidates, key=snapshot_sort_key)


def comparable_snapshot_paths(
    args: argparse.Namespace,
    current_path: Path,
    lookback_days: int,
) -> list[Path]:
    current_date = parse_snapshot_date(current_path)
    history_dir = Path(args.history_dir)
    if current_date is None or not history_dir.exists():
        return [current_path] if current_path.exists() else []

    earliest_date = current_date - dt.timedelta(days=lookback_days)
    pattern = (
        f"screened_stocks_*"
        f"_d{args.lookback_days}"
        f"_lu{args.min_limit_ups}"
        f"_inst{args.min_institutions}.csv"
    )
    paths: dict[Path, Path] = {}
    for path in history_dir.glob(pattern):
        snapshot_date = parse_snapshot_date(path)
        if snapshot_date is None or snapshot_date < earliest_date or snapshot_date > current_date:
            continue
        paths[path.resolve()] = path

    if current_path.exists():
        paths[current_path.resolve()] = current_path
    return sorted(paths.values(), key=snapshot_sort_key)


def build_recent_window_changes(
    args: argparse.Namespace,
    current_path: Path,
    current_rows: list[ScreenedStock],
    days: int = 5,
) -> RecentWindowChanges:
    paths = comparable_snapshot_paths(args, current_path, days)
    current_date = parse_snapshot_date(current_path)
    baseline_candidates = [path for path in paths if path.resolve() != current_path.resolve()]
    baseline_path = baseline_candidates[0] if baseline_candidates else None
    baseline_date = parse_snapshot_date(baseline_path) if baseline_path else None

    if baseline_path is None:
        return RecentWindowChanges(
            days=days,
            current_path=current_path,
            baseline_path=None,
            current_date=current_date,
            baseline_date=None,
            added=[],
            removed=[],
            note=f"未找到最近 {days} 日内的同口径历史快照，暂无法计算新增和消失股票。",
        )

    baseline_rows = read_screened_csv(baseline_path)
    current_by_code = {row.code: row for row in current_rows}
    baseline_by_code = {row.code: row for row in baseline_rows}
    added = [
        stock_change("近5日新增", current=current_by_code[code], previous=None)
        for code in sorted(current_by_code.keys() - baseline_by_code.keys())
    ]
    removed = [
        stock_change("近5日消失", current=None, previous=baseline_by_code[code])
        for code in sorted(baseline_by_code.keys() - current_by_code.keys())
    ]
    return RecentWindowChanges(
        days=days,
        current_path=current_path,
        baseline_path=baseline_path,
        current_date=current_date,
        baseline_date=baseline_date,
        added=sorted(added, key=change_sort_key, reverse=True),
        removed=sorted(removed, key=change_sort_key, reverse=True),
    )


def build_stock_histories(
    args: argparse.Namespace,
    current_path: Path,
    rows: list[ScreenedStock],
    days: int = 30,
) -> dict[str, list[StockHistoryPoint]]:
    histories = {row.code: [] for row in rows}
    if not histories:
        return histories

    for path in comparable_snapshot_paths(args, current_path, days):
        snapshot_date = parse_snapshot_date(path)
        if snapshot_date is None:
            continue
        for row in read_screened_csv(path):
            if row.code not in histories:
                continue
            histories[row.code].append(
                StockHistoryPoint(
                    snapshot_date=snapshot_date,
                    snapshot_path=path,
                    turnover_amount_100m=row.turnover_amount_100m,
                    institution_count=row.institution_count,
                    limit_up_count=row.limit_up_count,
                )
            )
    return histories


def parse_snapshot_date(path: Path) -> dt.date | None:
    match = SNAPSHOT_FILENAME_RE.match(path.name)
    if match is None:
        return None
    return dt.datetime.strptime(match.group(1), "%Y%m%d").date()


def snapshot_sort_key(path: Path) -> tuple[dt.date, float]:
    return (parse_snapshot_date(path) or dt.date.min, path.stat().st_mtime)


def compare_snapshots(
    args: argparse.Namespace,
    current_path: Path,
    current_rows: list[ScreenedStock],
    previous_path: Path,
    previous_rows: list[ScreenedStock],
    generated_at: str,
) -> AnalysisReport:
    current_by_code = {row.code: row for row in current_rows}
    previous_by_code = {row.code: row for row in previous_rows}

    added = [
        stock_change("新增", current=current_by_code[code], previous=None)
        for code in sorted(current_by_code.keys() - previous_by_code.keys())
    ]
    removed = [
        stock_change("删除", current=None, previous=previous_by_code[code])
        for code in sorted(previous_by_code.keys() - current_by_code.keys())
    ]
    institution_increased: list[StockChange] = []
    institution_decreased: list[StockChange] = []
    for code in sorted(current_by_code.keys() & previous_by_code.keys()):
        current = current_by_code[code]
        previous = previous_by_code[code]
        delta = current.institution_count - previous.institution_count
        if delta > 0:
            institution_increased.append(stock_change("机构数增加", current, previous))
        elif delta < 0:
            institution_decreased.append(stock_change("机构数减少", current, previous))

    return AnalysisReport(
        generated_at=generated_at,
        current_path=current_path,
        previous_path=previous_path,
        lookback_days=args.lookback_days,
        min_limit_ups=args.min_limit_ups,
        min_institutions=args.min_institutions,
        current_count=len(current_rows),
        previous_count=len(previous_rows),
        added=sorted(added, key=change_sort_key, reverse=True),
        removed=sorted(removed, key=change_sort_key, reverse=True),
        institution_increased=sorted(
            institution_increased,
            key=lambda change: abs(change.institution_count_delta or 0),
            reverse=True,
        ),
        institution_decreased=sorted(
            institution_decreased,
            key=lambda change: abs(change.institution_count_delta or 0),
            reverse=True,
        ),
    )


def stock_change(
    change_type: str,
    current: ScreenedStock | None,
    previous: ScreenedStock | None,
) -> StockChange:
    row = current or previous
    if row is None:
        raise ValueError("current 和 previous 不能同时为空")
    current_turnover_amount_100m = current.turnover_amount_100m if current is not None else None
    previous_turnover_amount_100m = previous.turnover_amount_100m if previous is not None else None
    turnover_delta = (
        current_turnover_amount_100m - previous_turnover_amount_100m
        if current_turnover_amount_100m is not None and previous_turnover_amount_100m is not None
        else None
    )
    current_institution_count = current.institution_count if current is not None else None
    previous_institution_count = previous.institution_count if previous is not None else None
    delta = (
        current_institution_count - previous_institution_count
        if current_institution_count is not None and previous_institution_count is not None
        else None
    )
    return StockChange(
        change_type=change_type,
        code=row.code,
        name=row.name,
        current_turnover_amount_100m=current_turnover_amount_100m,
        previous_turnover_amount_100m=previous_turnover_amount_100m,
        turnover_amount_delta_100m=turnover_delta,
        current_institution_count=current_institution_count,
        previous_institution_count=previous_institution_count,
        institution_count_delta=delta,
        current_limit_up_count=current.limit_up_count if current is not None else None,
        previous_limit_up_count=previous.limit_up_count if previous is not None else None,
        current_holding_report_date=current.holding_report_date if current is not None else "",
        previous_holding_report_date=previous.holding_report_date if previous is not None else "",
    )


def change_sort_key(change: StockChange) -> tuple[int, int, str]:
    return (
        change.current_limit_up_count or change.previous_limit_up_count or 0,
        change.current_institution_count or change.previous_institution_count or 0,
        change.code,
    )


def analysis_fieldnames() -> list[str]:
    return [
        "变化类型",
        "股票代码",
        "股票名称",
        "当前资金量(亿元)",
        "历史资金量(亿元)",
        "资金量变化(亿元)",
        "当前机构数",
        "历史机构数",
        "机构数变化",
        "当前涨停次数",
        "历史涨停次数",
        "当前机构报告期",
        "历史机构报告期",
    ]


def write_analysis_csv(path: Path, report: AnalysisReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=analysis_fieldnames())
        writer.writeheader()
        for change in all_changes(report):
            writer.writerow(change_to_output_dict(change))


def write_analysis_markdown(path: Path, report: AnalysisReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# 机构涨停候选股变化分析",
        "",
        f"- 生成时间: {report.generated_at}",
        (
            "- 筛选口径: "
            f"最近 {report.lookback_days} 天涨停次数 > {report.min_limit_ups}，"
            f"机构数 > {report.min_institutions}"
        ),
        f"- 当前快照: {report.current_path}",
        f"- 历史快照: {report.previous_path if report.previous_path else '无'}",
        "",
        "## 汇总",
        "",
        "| 指标 | 数量 |",
        "| --- | ---: |",
        f"| 当前股票数 | {report.current_count} |",
        f"| 历史股票数 | {report.previous_count} |",
        f"| 新增股票 | {len(report.added)} |",
        f"| 删除股票 | {len(report.removed)} |",
        f"| 机构数增加 | {len(report.institution_increased)} |",
        f"| 机构数减少 | {len(report.institution_decreased)} |",
    ]
    if report.note:
        lines.extend(["", f"> {report.note}"])

    append_change_section(lines, "新增股票", report.added)
    append_change_section(lines, "删除股票", report.removed)
    append_change_section(lines, "机构数增加", report.institution_increased)
    append_change_section(lines, "机构数减少", report.institution_decreased)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_analysis_html(path: Path, report: AnalysisReport) -> None:
    metric_cards = [
        ("生成时间", report.generated_at),
        ("当前股票数", str(report.current_count)),
        ("历史股票数", str(report.previous_count)),
        ("新增股票", str(len(report.added))),
        ("删除股票", str(len(report.removed))),
        ("机构数增加", str(len(report.institution_increased))),
        ("机构数减少", str(len(report.institution_decreased))),
    ]
    sections = [
        render_change_html_section("新增股票", report.added),
        render_change_html_section("删除股票", report.removed),
        render_change_html_section("机构数增加", report.institution_increased),
        render_change_html_section("机构数减少", report.institution_decreased),
    ]
    note = f'<div class="note">{html_escape(report.note)}</div>' if report.note else ""
    body = [
        html_header(
            title="机构涨停候选股变化分析",
            subtitle=(
                f"最近 {report.lookback_days} 天涨停次数 > {report.min_limit_ups}，"
                f"机构数 > {report.min_institutions}"
            ),
        ),
        render_metric_cards(metric_cards),
        render_source_links(report),
        note,
        "\n".join(sections),
        '<p class="disclaimer">仅供研究，不构成投资建议。</p>',
    ]
    write_html_document(path, "机构涨停候选股变化分析", "\n".join(part for part in body if part))


def render_change_html_section(title: str, changes: list[StockChange]) -> str:
    if not changes:
        return (
            f'<section class="panel"><h2>{html_escape(title)}</h2>'
            '<div class="empty">无变化</div></section>'
        )

    row_classes = [change_html_class(change) for change in changes]
    records = [change_to_output_dict(change) for change in changes]
    return (
        f'<section class="panel"><h2>{html_escape(title)}</h2>'
        f"{render_html_table(analysis_fieldnames(), records, row_classes=row_classes)}"
        "</section>"
    )


def write_stock_detail_pages(
    detail_dir: Path,
    rows: list[ScreenedStock],
    histories: dict[str, list[StockHistoryPoint]],
    args: argparse.Namespace,
) -> int:
    detail_dir.mkdir(parents=True, exist_ok=True)
    for row in rows:
        write_stock_detail_html(
            detail_dir / f"{row.code}.html",
            row,
            histories.get(row.code, []),
            args,
        )
    return len(rows)


def write_stock_detail_html(
    path: Path,
    row: ScreenedStock,
    history: list[StockHistoryPoint],
    args: argparse.Namespace,
) -> None:
    current_history = history[-1] if history else None
    coverage = (
        f"{history[0].snapshot_date:%Y-%m-%d} 至 {history[-1].snapshot_date:%Y-%m-%d}"
        if history
        else "暂无可比历史快照"
    )
    combined_chart = render_combined_history_chart(history)
    latest_turnover = format_number(current_history.turnover_amount_100m) if current_history else ""
    body = [
        html_header(
            title=f"{row.code} {row.name}",
            subtitle=(
                f"近 30 日候选快照趋势；筛选口径为最近 {args.lookback_days} 天涨停次数 "
                f"> {args.min_limit_ups}，机构数 > {args.min_institutions}"
            ),
        ),
        '<nav class="breadcrumb"><a href="../latest/index.html">返回候选股主页</a></nav>',
        render_metric_cards(
            [
                ("所属行业", row.industry or "未取到"),
                ("最近资金量", f"{latest_turnover or '-'} 亿元"),
                ("当前机构数", f"{row.institution_count} 家"),
                ("当前涨停次数", f"{row.limit_up_count} 次"),
                ("历史覆盖", coverage),
            ]
        ),
        '<section class="panel chart-grid">' + combined_chart + "</section>",
        render_stock_history_table(history),
        '<p class="disclaimer">图表基于每日归档快照生成；若某日没有同口径快照，该日不会出现采样点。</p>',
    ]
    write_html_document(path, f"{row.code} {row.name} 详情", "\n".join(body))


def render_combined_history_chart(history: list[StockHistoryPoint]) -> str:
    if not history:
        return (
            '<article class="chart-card">'
            "<h2>近 30 日三指标变化</h2>"
            '<div class="empty">暂无可绘制数据</div>'
            "</article>"
        )

    series_definitions = [
        ("资金量", "亿元", "#b25c2a", lambda point: point.turnover_amount_100m),
        ("机构数量", "家", "#245d73", lambda point: float(point.institution_count)),
        ("涨停次数", "次", "#7a5a16", lambda point: float(point.limit_up_count)),
    ]
    usable_series = []
    for label, unit, color, extractor in series_definitions:
        values = [(point, extractor(point)) for point in history]
        filtered = [(point, float(value)) for point, value in values if value is not None]
        if filtered:
            usable_series.append((label, unit, color, filtered))

    if not usable_series:
        return (
            '<article class="chart-card">'
            "<h2>近 30 日三指标变化</h2>"
            '<div class="empty">暂无可绘制数据</div>'
            "</article>"
        )

    width = 980
    height = 360
    left = 72
    right = 28
    top = 42
    bottom = 62
    plot_width = width - left - right
    plot_height = height - top - bottom

    grid_lines = []
    for step in range(5):
        ratio = step / 4
        y = top + plot_height * ratio
        score = int(round((1 - ratio) * 100))
        grid_lines.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{width-right}" y2="{y:.2f}" class="chart-gridline" />'
            f'<text x="{left-10}" y="{y+4:.2f}" class="chart-axis-label">{score}%</text>'
        )

    all_dates = [point.snapshot_date for point in history]
    date_to_x = {}
    for index, snapshot_date in enumerate(all_dates):
        x = left + plot_width / 2 if len(all_dates) == 1 else left + plot_width * index / (len(all_dates) - 1)
        date_to_x[snapshot_date] = x

    date_labels = [
        f'<text x="{date_to_x[snapshot_date]:.2f}" y="{height-bottom+28}" class="chart-date">'
        f"{snapshot_date:%m-%d}</text>"
        for snapshot_date in all_dates
    ]

    paths = []
    markers = []
    legend_items = []
    for label, unit, color, values in usable_series:
        raw_values = [value for _, value in values]
        minimum = min(raw_values)
        maximum = max(raw_values)
        value_range = maximum - minimum
        positions = []
        for point, value in values:
            normalized = 0.5 if value_range == 0 else (value - minimum) / value_range
            x = date_to_x[point.snapshot_date]
            y = top + plot_height * (1 - normalized)
            positions.append((point, value, x, y))
        polyline_points = " ".join(f"{x:.2f},{y:.2f}" for _, _, x, y in positions)
        paths.append(
            f'<polyline points="{polyline_points}" fill="none" stroke="{html_escape(color)}" '
            'stroke-width="4" stroke-linecap="round" stroke-linejoin="round" />'
        )
        for point, value, x, y in positions:
            markers.append(
                f'<circle cx="{x:.2f}" cy="{y:.2f}" r="5.5" fill="{html_escape(color)}">'
                f"<title>{html_escape(label)} / {point.snapshot_date:%Y-%m-%d}: "
                f"{html_escape(format_chart_value(value))} {html_escape(unit)}</title>"
                "</circle>"
            )
        latest_value = format_chart_value(positions[-1][1])
        legend_items.append(
            '<li>'
            f'<span class="legend-swatch" style="--legend-color:{html_escape(color)}"></span>'
            f"<strong>{html_escape(label)}</strong>"
            f"<span>最新 {html_escape(latest_value)} {html_escape(unit)}</span>"
            "</li>"
        )

    svg = (
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="资金量、机构数量、涨停次数变化">'
        f"{''.join(grid_lines)}"
        f'<line x1="{left}" y1="{height-bottom}" x2="{width-right}" y2="{height-bottom}" class="chart-axis" />'
        f"{''.join(paths)}"
        f"{''.join(markers)}"
        f"{''.join(date_labels)}"
        "</svg>"
    )
    return (
        '<article class="chart-card combined-chart">'
        "<div>"
        "<h2>近 30 日三指标变化</h2>"
        '<p class="chart-summary">同图采用各指标自身区间归一化，便于比较走势方向；悬停节点可查看原始值。</p>'
        f"{svg}"
        "</div>"
        '<aside class="chart-legend"><h3>图例</h3><ul>'
        f"{''.join(legend_items)}"
        "</ul></aside>"
        "</article>"
    )


def render_stock_history_table(history: list[StockHistoryPoint]) -> str:
    records: list[dict[str, object]] = []
    for point in history:
        records.append(
            {
                "快照日期": f"{point.snapshot_date:%Y-%m-%d}",
                "资金量(亿元)": format_number(point.turnover_amount_100m),
                "机构数": point.institution_count,
                "涨停次数": point.limit_up_count,
                "快照文件": str(point.snapshot_path),
            }
        )
    return (
        '<section class="panel"><h2>近 30 日历史采样</h2>'
        f"{render_html_table(['快照日期', '资金量(亿元)', '机构数', '涨停次数', '快照文件'], records)}"
        "</section>"
    )


def format_chart_value(value: float) -> str:
    if abs(value - round(value)) < 0.005:
        return str(int(round(value)))
    return f"{value:.2f}"


def append_change_section(lines: list[str], title: str, changes: list[StockChange]) -> None:
    lines.extend(["", f"## {title}", ""])
    if not changes:
        lines.append("无")
        return

    fieldnames = analysis_fieldnames()
    lines.extend(
        [
            "| " + " | ".join(fieldnames) + " |",
            "| " + " | ".join(["---"] * len(fieldnames)) + " |",
        ]
    )
    for change in changes:
        record = change_to_output_dict(change)
        lines.append("| " + " | ".join(markdown_cell(record[name]) for name in fieldnames) + " |")


def all_changes(report: AnalysisReport) -> list[StockChange]:
    return (
        report.added
        + report.removed
        + report.institution_increased
        + report.institution_decreased
    )


def change_to_output_dict(change: StockChange) -> dict[str, str]:
    return {
        "变化类型": change.change_type,
        "股票代码": change.code,
        "股票名称": change.name,
        "当前资金量(亿元)": format_number(change.current_turnover_amount_100m),
        "历史资金量(亿元)": format_number(change.previous_turnover_amount_100m),
        "资金量变化(亿元)": format_float_delta(change.turnover_amount_delta_100m),
        "当前机构数": format_optional_int(change.current_institution_count),
        "历史机构数": format_optional_int(change.previous_institution_count),
        "机构数变化": format_delta(change.institution_count_delta),
        "当前涨停次数": format_optional_int(change.current_limit_up_count),
        "历史涨停次数": format_optional_int(change.previous_limit_up_count),
        "当前机构报告期": change.current_holding_report_date,
        "历史机构报告期": change.previous_holding_report_date,
    }


def write_html_document(path: Path, title: str, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = (
        "<!doctype html>\n"
        '<html lang="zh-CN">\n'
        "<head>\n"
        '<meta charset="utf-8">\n'
        '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"<title>{html_escape(title)}</title>\n"
        "<style>\n"
        f"{html_styles()}\n"
        "</style>\n"
        "</head>\n"
        "<body>\n"
        '<main class="page">\n'
        f"{body}\n"
        "</main>\n"
        f"{render_theme_toggle_script()}\n"
        f"{render_sticky_table_header_script()}\n"
        "</body>\n"
        "</html>\n"
    )
    path.write_text(content, encoding="utf-8")


def html_styles() -> str:
    return """
:root {
  --bg: #f5efe3;
  --panel: #fffaf0;
  --panel-strong: rgba(255, 250, 240, 0.96);
  --panel-soft: rgba(255, 250, 240, 0.82);
  --panel-muted: rgba(255, 250, 240, 0.62);
  --surface: #fffefa;
  --ink: #17202a;
  --muted: #667085;
  --line: #dfd4bf;
  --soft-line: #eadfce;
  --brand: #9a4f22;
  --brand-dark: #5a2f17;
  --added: #e3f7df;
  --removed: #ffe3df;
  --up: #fff3c4;
  --down: #e0ecff;
  --body-glow-warm: rgba(199, 119, 54, 0.20);
  --body-glow-cool: rgba(35, 83, 116, 0.16);
  --body-top: #fbf6ec;
  --hero-shadow: rgba(64, 42, 24, 0.13);
  --hero-accent: rgba(246, 226, 192, 0.72);
  --panel-shadow: rgba(64, 42, 24, 0.08);
  --floating-shadow: rgba(64, 42, 24, 0.15);
  --focus-ring: rgba(154, 79, 34, 0.36);
  --note-bg: rgba(255, 247, 224, 0.9);
  --row-even: rgba(247, 240, 226, 0.55);
  --row-hover: #fff5d7;
  --chip-bg: rgba(255, 254, 250, 0.92);
  --tag-count-bg: rgba(255, 255, 255, 0.58);
  --theme-button-bg: rgba(255, 254, 250, 0.92);
  --theme-button-ink: var(--brand-dark);
}
:root[data-theme="dark"] {
  --bg: #15191f;
  --panel: #20262f;
  --panel-strong: rgba(32, 38, 47, 0.97);
  --panel-soft: rgba(32, 38, 47, 0.92);
  --panel-muted: rgba(32, 38, 47, 0.78);
  --surface: #1d232b;
  --ink: #edf2f7;
  --muted: #a8b3c2;
  --line: #3d4654;
  --soft-line: #39414d;
  --brand: #df9560;
  --brand-dark: #a95f35;
  --added: #214132;
  --removed: #4b2828;
  --up: #4a4020;
  --down: #223852;
  --body-glow-warm: rgba(223, 149, 96, 0.16);
  --body-glow-cool: rgba(72, 128, 180, 0.18);
  --body-top: #1b2027;
  --hero-shadow: rgba(0, 0, 0, 0.32);
  --hero-accent: rgba(169, 95, 53, 0.20);
  --panel-shadow: rgba(0, 0, 0, 0.24);
  --floating-shadow: rgba(0, 0, 0, 0.34);
  --focus-ring: rgba(223, 149, 96, 0.42);
  --note-bg: rgba(223, 149, 96, 0.12);
  --row-even: rgba(255, 255, 255, 0.035);
  --row-hover: rgba(223, 149, 96, 0.12);
  --chip-bg: rgba(28, 34, 42, 0.94);
  --tag-count-bg: rgba(255, 255, 255, 0.12);
  --theme-button-bg: rgba(28, 34, 42, 0.94);
  --theme-button-ink: #fff4eb;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  color: var(--ink);
  background:
    radial-gradient(circle at 12% 8%, var(--body-glow-warm), transparent 28rem),
    radial-gradient(circle at 88% 0%, var(--body-glow-cool), transparent 24rem),
    linear-gradient(135deg, var(--body-top), var(--bg));
  font-family: "Noto Serif SC", "Microsoft YaHei", "Noto Sans CJK SC", serif;
}
.page {
  width: min(1540px, calc(100% - 32px));
  margin: 0 auto;
  padding: 28px 0 40px;
}
.hero {
  border: 1px solid rgba(154, 79, 34, 0.22);
  border-radius: 24px;
  padding: 28px;
  background: linear-gradient(135deg, var(--panel-strong), var(--hero-accent));
  box-shadow: 0 18px 50px var(--hero-shadow);
}
.eyebrow {
  margin: 0 0 10px;
  color: var(--brand);
  font-size: 13px;
  font-weight: 700;
  letter-spacing: 0.12em;
}
.theme-toggle {
  display: inline-flex;
  width: 42px;
  height: 42px;
  align-items: center;
  justify-content: center;
  border: 1px solid rgba(90, 47, 23, 0.22);
  border-radius: 999px;
  color: var(--theme-button-ink);
  background: var(--theme-button-bg);
  box-shadow: 0 10px 24px var(--panel-shadow);
  cursor: pointer;
}
.theme-toggle:hover {
  color: #fffaf0;
  background: linear-gradient(135deg, var(--brand), var(--brand-dark));
}
.theme-toggle:focus-visible {
  outline: 3px solid var(--focus-ring);
  outline-offset: 3px;
}
.theme-toggle svg {
  width: 20px;
  height: 20px;
}
.theme-toggle .icon-moon { display: none; }
:root[data-theme="dark"] .theme-toggle .icon-sun { display: none; }
:root[data-theme="dark"] .theme-toggle .icon-moon { display: block; }
.jump-nav .theme-toggle {
  margin: 0 auto;
}
h1 {
  margin: 0;
  font-size: clamp(28px, 4vw, 46px);
  line-height: 1.15;
}
.subtitle {
  margin: 12px 0 0;
  color: var(--muted);
  font-size: 16px;
}
.metrics {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
  gap: 12px;
  margin: 18px 0;
}
.metric {
  padding: 15px 16px;
  border: 1px solid var(--line);
  border-radius: 18px;
  background: var(--panel-strong);
}
.metric-label {
  color: var(--muted);
  font-size: 12px;
}
.metric-value {
  margin-top: 6px;
  font-size: 18px;
  font-weight: 800;
  word-break: break-word;
}
.panel,
.sources,
.note,
.table-wrap {
  margin-top: 18px;
}
.panel {
  padding: 18px;
  border: 1px solid var(--line);
  border-radius: 20px;
  background: var(--panel-soft);
}
.concept-cloud-panel {
  display: grid;
  gap: 12px;
}
.concept-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}
.concept-summary {
  color: var(--muted);
  font-size: 13px;
}
.concept-toggle {
  border: 1px solid rgba(90, 47, 23, 0.22);
  border-radius: 999px;
  padding: 6px 10px;
  color: var(--brand-dark);
  background: var(--chip-bg);
  font: inherit;
  font-size: 12px;
  font-weight: 800;
  cursor: pointer;
}
.concept-toggle:hover {
  color: #fffaf0;
  background: linear-gradient(135deg, var(--brand), var(--brand-dark));
}
.concept-cloud {
  display: flex;
  flex-wrap: wrap;
  gap: 7px;
}
.concept-tag {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  border: 1px solid rgba(90, 47, 23, 0.22);
  border-radius: 999px;
  padding: 6px 10px;
  color: var(--concept-ink, var(--brand-dark));
  background: var(--concept-bg, var(--chip-bg));
  font: inherit;
  font-size: 12px;
  font-weight: 800;
  cursor: pointer;
}
.concept-tag[hidden] {
  display: none;
}
.concept-tag span {
  display: inline-flex;
  min-width: 18px;
  justify-content: center;
  border-radius: 999px;
  padding: 1px 5px;
  color: var(--concept-ink, #fffaf0);
  background: var(--tag-count-bg);
  font-size: 11px;
}
.concept-tag:hover {
  filter: brightness(0.98);
}
.concept-tag.is-active {
  border-color: var(--brand-dark);
  box-shadow: inset 0 0 0 2px rgba(90, 47, 23, 0.18), 0 8px 18px rgba(64, 42, 24, 0.12);
}
.concept-cell {
  display: flex;
  flex-wrap: wrap;
  gap: 3px;
  min-width: 190px;
}
.concept-cell-tag {
  display: inline-flex;
  align-items: center;
  border-radius: 999px;
  padding: 2px 6px;
  color: var(--concept-ink, var(--brand-dark));
  background: var(--concept-bg, var(--chip-bg));
  font-size: 11px;
  line-height: 1.25;
  font-weight: 800;
  white-space: nowrap;
}
.concept-cell-tag.is-selected-match {
  outline: 1px solid var(--brand-dark);
  outline-offset: 1px;
  box-shadow: 0 0 0 3px rgba(154, 79, 34, 0.12);
}
h2 {
  margin: 0 0 12px;
  font-size: 22px;
}
.sources,
.note,
.disclaimer {
  color: var(--muted);
  font-size: 13px;
}
.sources {
  display: grid;
  gap: 6px;
  padding: 14px 16px;
  border: 1px dashed var(--line);
  border-radius: 16px;
  background: var(--panel-muted);
}
.dashboard-note { margin-bottom: 0; }
.jump-nav {
  position: fixed;
  right: max(18px, calc((100vw - min(1540px, calc(100vw - 32px))) / 2 - 104px));
  top: 188px;
  z-index: 8;
  display: grid;
  gap: 10px;
  padding: 12px;
  border: 1px solid rgba(90, 47, 23, 0.18);
  border-radius: 20px;
  background: var(--panel-strong);
  box-shadow: 0 16px 40px var(--floating-shadow);
  backdrop-filter: blur(12px);
}
.jump-nav a {
  min-width: 72px;
  padding: 10px 12px;
  border-radius: 999px;
  color: #fffaf0;
  background: linear-gradient(135deg, var(--brand), var(--brand-dark));
  font-weight: 800;
  text-align: center;
  text-decoration: none;
}
.jump-nav a:hover {
  filter: brightness(1.08);
}
.sticky-table-header {
  position: fixed;
  top: 0;
  z-index: 20;
  display: none;
  overflow: hidden;
  border: 1px solid var(--line);
  border-top: 0;
  border-radius: 0 0 14px 14px;
  background: var(--brand-dark);
  box-shadow: 0 12px 28px var(--floating-shadow);
  pointer-events: none;
}
.sticky-table-header table {
  min-width: 0;
}
.sticky-table-header th {
  position: static;
}
.note {
  padding: 13px 16px;
  border-left: 4px solid var(--brand);
  border-radius: 12px;
  background: var(--note-bg);
}
.table-wrap {
  overflow-x: auto;
  border: 1px solid var(--line);
  border-radius: 18px;
  background: var(--surface);
  box-shadow: 0 10px 30px var(--panel-shadow);
}
table {
  width: 100%;
  min-width: 1120px;
  border-collapse: separate;
  border-spacing: 0;
}
th,
td {
  padding: 10px 11px;
  border-bottom: 1px solid var(--soft-line);
  vertical-align: top;
  text-align: left;
  font-size: 13px;
  line-height: 1.48;
}
th {
  position: sticky;
  top: 0;
  z-index: 1;
  color: #fffaf0;
  background: var(--brand-dark);
  white-space: nowrap;
}
tbody tr:nth-child(even) td { background: var(--row-even); }
tbody tr:hover td { background: var(--row-hover); }
.row-added td { background: var(--added) !important; }
.row-removed td { background: var(--removed) !important; }
.row-up td { background: var(--up) !important; }
.row-down td { background: var(--down) !important; }
.detail-link,
.breadcrumb a {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border: 1px solid rgba(90, 47, 23, 0.22);
  border-radius: 999px;
  padding: 7px 13px;
  color: #fffaf0;
  background: linear-gradient(135deg, var(--brand), var(--brand-dark));
  font-weight: 800;
  text-decoration: none;
  white-space: nowrap;
}
.detail-link:hover,
.breadcrumb a:hover {
  filter: brightness(1.08);
}
.breadcrumb {
  margin: 18px 0 0;
}
.chart-grid {
  display: grid;
  gap: 18px;
}
.chart-card {
  overflow: hidden;
  border: 1px solid var(--line);
  border-radius: 18px;
  padding: 16px;
  background: var(--panel-strong);
}
.combined-chart {
  display: grid;
  grid-template-columns: minmax(0, 1fr) 250px;
  gap: 18px;
  align-items: start;
}
.chart-card h2 {
  margin-bottom: 4px;
}
.chart-legend {
  border: 1px solid var(--line);
  border-radius: 18px;
  padding: 16px;
  background: rgba(255, 250, 240, 0.88);
}
.chart-legend h3 {
  margin: 0 0 12px;
  font-size: 18px;
}
.chart-legend ul {
  display: grid;
  gap: 12px;
  margin: 0;
  padding: 0;
  list-style: none;
}
.chart-legend li {
  display: grid;
  grid-template-columns: 16px 1fr;
  column-gap: 10px;
  row-gap: 4px;
  color: var(--muted);
  font-size: 13px;
}
.chart-legend strong {
  grid-column: 2;
  color: var(--ink);
  font-size: 14px;
}
.chart-legend span:last-child {
  grid-column: 2;
}
.legend-swatch {
  grid-row: span 2;
  align-self: center;
  width: 14px;
  height: 14px;
  border-radius: 999px;
  background: var(--legend-color);
  box-shadow: 0 0 0 4px rgba(90, 47, 23, 0.08);
}
.chart-summary {
  margin: 0 0 8px;
  color: var(--muted);
  font-size: 13px;
}
.chart-card svg {
  display: block;
  width: 100%;
  height: auto;
}
.chart-gridline {
  stroke: rgba(90, 47, 23, 0.12);
  stroke-width: 1;
}
.chart-axis {
  stroke: rgba(90, 47, 23, 0.42);
  stroke-width: 1.5;
}
.chart-axis-label,
.chart-date {
  fill: var(--muted);
  font-size: 12px;
}
.chart-axis-label {
  text-anchor: end;
}
.chart-date {
  text-anchor: middle;
}
.empty {
  padding: 20px;
  border: 1px dashed var(--line);
  border-radius: 16px;
  color: var(--muted);
  background: rgba(255, 250, 240, 0.65);
}
.disclaimer { margin: 18px 2px 0; }
@media (max-width: 720px) {
  .page { width: min(100% - 18px, 1540px); padding-top: 12px; }
  .hero { padding: 20px; border-radius: 18px; }
  .metric-value { font-size: 16px; }
  th, td { padding: 8px; font-size: 12px; }
  .concept-toolbar {
    align-items: flex-start;
    flex-direction: column;
  }
  .concept-tag {
    padding: 5px 9px;
  }
  .concept-toggle {
    padding: 5px 9px;
  }
  .jump-nav {
    position: sticky;
    top: 8px;
    right: auto;
    display: flex;
    justify-content: center;
    margin-top: 14px;
  }
  .combined-chart {
    grid-template-columns: 1fr;
  }
}
""".strip()


def html_header(title: str, subtitle: str) -> str:
    return (
        '<section class="hero">'
        '<p class="eyebrow">GPGETTER DAILY REPORT</p>'
        f"<h1>{html_escape(title)}</h1>"
        f'<p class="subtitle">{html_escape(subtitle)}</p>'
        "</section>"
    )


def render_theme_toggle_button() -> str:
    return (
        '<button class="theme-toggle" type="button" data-theme-toggle '
        'aria-label="切换到深色主题" title="切换到深色主题" aria-pressed="false">'
        '<svg class="icon-sun" viewBox="0 0 24 24" fill="none" aria-hidden="true">'
        '<circle cx="12" cy="12" r="4" stroke="currentColor" stroke-width="2"/>'
        '<path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41" '
        'stroke="currentColor" stroke-width="2" stroke-linecap="round"/>'
        '</svg>'
        '<svg class="icon-moon" viewBox="0 0 24 24" fill="none" aria-hidden="true">'
        '<path d="M20.5 15.3A8.5 8.5 0 0 1 8.7 3.5 9 9 0 1 0 20.5 15.3Z" '
        'stroke="currentColor" stroke-width="2" stroke-linejoin="round"/>'
        '</svg>'
        '</button>'
    )


def render_metric_cards(items: list[tuple[str, str]]) -> str:
    cards = []
    for label, value in items:
        cards.append(
            '<div class="metric">'
            f'<div class="metric-label">{html_escape(label)}</div>'
            f'<div class="metric-value">{html_escape(value)}</div>'
            "</div>"
        )
    return '<section class="metrics">' + "\n".join(cards) + "</section>"


def render_source_links(report: AnalysisReport) -> str:
    previous = str(report.previous_path) if report.previous_path else "无"
    return (
        '<section class="sources">'
        f"<div>当前快照: {html_escape(report.current_path)}</div>"
        f"<div>历史快照: {html_escape(previous)}</div>"
        "</section>"
    )


def render_html_table(
    fieldnames: list[str],
    records: list[dict[str, object]],
    row_classes: list[str] | None = None,
    raw_fields: set[str] | None = None,
) -> str:
    if not records:
        return '<div class="empty">无数据</div>'

    headers = "".join(f"<th>{html_escape(name)}</th>" for name in fieldnames)
    rows = []
    for index, record in enumerate(records):
        row_class = ""
        if row_classes is not None and index < len(row_classes) and row_classes[index]:
            row_class = f' class="{html_escape(row_classes[index])}"'
        cells = []
        for name in fieldnames:
            value = record.get(name, "")
            rendered = str(value) if raw_fields and name in raw_fields else html_escape(value)
            cells.append(f"<td>{rendered}</td>")
        rows.append(f"<tr{row_class}>{''.join(cells)}</tr>")

    return (
        '<div class="table-wrap"><table>'
        f"<thead><tr>{headers}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table></div>"
    )


def render_candidate_html_table(
    fieldnames: list[str],
    records: list[dict[str, object]],
    raw_fields: set[str] | None = None,
) -> str:
    if not records:
        return '<div class="empty">无数据</div>'

    headers = "".join(f"<th>{html_escape(name)}</th>" for name in fieldnames)
    rows = []
    for record in records:
        tags = record.get("__concept_tags__", [])
        tag_attr = "|".join(str(tag) for tag in tags)
        cells = []
        for name in fieldnames:
            if name == "相关概念":
                rendered = render_concept_table_cell([str(tag) for tag in tags])
            else:
                value = record.get(name, "")
                rendered = str(value) if raw_fields and name in raw_fields else html_escape(value)
            cells.append(f"<td>{rendered}</td>")
        rows.append(
            f'<tr data-concepts="{html_escape(tag_attr)}">{"".join(cells)}</tr>'
        )

    return (
        '<div class="table-wrap"><table>'
        f"<thead><tr>{headers}</tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table></div>"
    )


def render_concept_table_cell(tags: list[str]) -> str:
    if not tags:
        return (
            '<div class="concept-cell">'
            f'<span class="concept-cell-tag concept-cell-empty" '
            f'data-concept-chip="{EMPTY_CONCEPT_FILTER}" '
            'style="--concept-bg:#e5e7eb;--concept-ink:#4b5563;">暂无</span>'
            "</div>"
        )

    rendered = []
    for tag in tags:
        rendered.append(
            '<span class="concept-cell-tag" '
            f'data-concept-chip="{html_escape(tag)}" '
            f'style="{concept_style_attr(tag)}">{html_escape(tag)}</span>'
        )
    return '<div class="concept-cell">' + "".join(rendered) + "</div>"


def change_html_class(change: StockChange) -> str:
    if change.change_type == "新增":
        return "row-added"
    if change.change_type == "删除":
        return "row-removed"
    if change.change_type == "机构数增加":
        return "row-up"
    if change.change_type == "机构数减少":
        return "row-down"
    return ""


def html_escape(value: object) -> str:
    return html.escape(str(value), quote=True)


def format_optional_int(value: int | None) -> str:
    return "" if value is None else str(value)


def format_delta(value: int | None) -> str:
    if value is None:
        return ""
    return f"{value:+d}"


def format_float_delta(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:+.2f}"


def markdown_cell(value: object) -> str:
    return str(value).replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")


def format_number(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.2f}"


def load_or_update_screening(args: argparse.Namespace, output_path: Path) -> list[ScreenedStock]:
    if args.analyze_only:
        if not output_path.exists():
            raise FileNotFoundError(f"分析模式需要先存在筛选结果: {output_path}")
        print(f"读取现有结果: {output_path.resolve()}")
        return read_screened_csv(output_path)

    if output_path.exists() and not args.force_update and not args.output_was_provided:
        print(f"发现当天同口径结果，跳过重复拉取: {output_path.resolve()}")
        return read_screened_csv(output_path)

    rows = screen_stocks(args)
    write_csv(output_path, rows)
    return rows


def run_once(args: argparse.Namespace) -> tuple[Path, Path | None, Path | None, int]:
    output_path = Path(args.output)
    rows = load_or_update_screening(args, output_path)
    write_markdown(LATEST_MARKDOWN_PATH, rows)
    stock_html_path: Path | None = None
    latest_stock_html_path: Path | None = None
    detail_pages_count = 0
    if not args.skip_html:
        recent_changes = build_recent_window_changes(args, output_path, rows)
        histories = build_stock_histories(args, output_path, rows)
        detail_dir = output_path.parent / "details"
        detail_pages_count = write_stock_detail_pages(detail_dir, rows, histories, args)
        stock_html_path = output_path.with_suffix(".html")
        write_stock_html(
            stock_html_path,
            rows,
            args,
            output_path,
            recent_changes,
            detail_href_prefix="details/",
        )
        latest_stock_html_path = LATEST_HTML_PATH
        if stock_html_path.resolve() != latest_stock_html_path.resolve():
            write_stock_html(
                latest_stock_html_path,
                rows,
                args,
                output_path,
                recent_changes,
                detail_href_prefix="../details/",
            )

    analysis_markdown_path: Path | None = None
    analysis_csv_path: Path | None = None
    analysis_html_path: Path | None = None
    latest_analysis_html_path: Path | None = None
    if not args.skip_analysis:
        report = build_analysis_report(args, output_path, rows)
        analysis_markdown_path = Path(args.analysis_output)
        analysis_csv_path = analysis_markdown_path.with_suffix(".csv")
        write_analysis_markdown(analysis_markdown_path, report)
        write_analysis_csv(analysis_csv_path, report)
        if not args.skip_html:
            analysis_html_path = analysis_markdown_path.with_suffix(".html")
            write_analysis_html(analysis_html_path, report)
            latest_analysis_html_path = LATEST_ANALYSIS_HTML_PATH
            if analysis_html_path.resolve() != latest_analysis_html_path.resolve():
                write_analysis_html(latest_analysis_html_path, report)
        if analysis_markdown_path.resolve() != LATEST_ANALYSIS_MARKDOWN_PATH.resolve():
            write_analysis_markdown(LATEST_ANALYSIS_MARKDOWN_PATH, report)

    print(f"筛选完成: {len(rows)} 只股票")
    print(f"结果文件: {output_path.resolve()}")
    print(f"Markdown 文件: {LATEST_MARKDOWN_PATH.resolve()}")
    if stock_html_path is not None:
        print(f"HTML 文件: {stock_html_path.resolve()}")
    if latest_stock_html_path is not None:
        print(f"最新 HTML: {latest_stock_html_path.resolve()}")
        print(f"详情页数量: {detail_pages_count}，目录: {(output_path.parent / 'details').resolve()}")
    if analysis_markdown_path is not None:
        print(f"分析文件: {analysis_markdown_path.resolve()}")
    if analysis_csv_path is not None:
        print(f"分析 CSV: {analysis_csv_path.resolve()}")
    if analysis_html_path is not None:
        print(f"分析 HTML: {analysis_html_path.resolve()}")
    if latest_analysis_html_path is not None:
        print(f"最新变化 HTML: {latest_analysis_html_path.resolve()}")
    print("口径: 沪深主板、非 ST、机构汇总持股家数 > "
          f"{args.min_institutions}、近 {args.lookback_days} 天涨停次数 > {args.min_limit_ups}")
    print("数据源: 东方财富机构持仓数据、腾讯日线行情数据，仅供研究，不构成投资建议。")
    return output_path, analysis_markdown_path, analysis_csv_path, len(rows)


def run_background(args: argparse.Namespace) -> int:
    print(f"后台模式启动，每 {args.background_interval_days} 天执行一次。")
    while True:
        apply_default_dates_and_paths(args, dt.date.today())
        try:
            run_once(args)
        except Exception as exc:
            print(f"[ERROR] 本轮后台任务失败: {exc}", file=sys.stderr)

        sleep_seconds = args.background_interval_days * 24 * 60 * 60
        next_run = dt.datetime.now() + dt.timedelta(seconds=sleep_seconds)
        print(f"下一次运行时间: {next_run:%Y-%m-%d %H:%M:%S}")
        time.sleep(sleep_seconds)


def build_backfill_dates(end_date: dt.date, days: int) -> list[dt.date]:
    start_date = end_date - dt.timedelta(days=days - 1)
    return [start_date + dt.timedelta(days=offset) for offset in range(days)]


def args_for_backfill_date(args: argparse.Namespace, snapshot_date: dt.date) -> argparse.Namespace:
    day_args = argparse.Namespace(**vars(args))
    day_args.end_date = f"{snapshot_date:%Y%m%d}"
    day_args.end_date_was_provided = True
    day_args.output_was_provided = False
    day_args.analysis_output_was_provided = False
    day_args.output = str(default_screening_output_path(day_args, snapshot_date))
    day_args.analysis_output = str(default_analysis_output_path(day_args, snapshot_date))
    day_args.backfill_days = None
    day_args.use_historical_turnover = True
    return day_args


def run_backfill(args: argparse.Namespace) -> int:
    end_date = parse_trade_date(args.end_date)
    dates = build_backfill_dates(end_date, args.backfill_days)
    print(
        f"开始回填 {len(dates)} 天快照: "
        f"{dates[0]:%Y-%m-%d} 至 {dates[-1]:%Y-%m-%d}"
    )
    for index, snapshot_date in enumerate(dates, start=1):
        print(f"\n[{index}/{len(dates)}] 回填 {snapshot_date:%Y-%m-%d}")
        run_once(args_for_backfill_date(args, snapshot_date))
    print(f"回填完成: {len(dates)} 天")
    return 0


def main() -> int:
    args = parse_args()
    if args.backfill_days is not None:
        return run_backfill(args)
    if args.background:
        return run_background(args)
    run_once(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
