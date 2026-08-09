#!/usr/bin/env python3
"""
Mirakurun / チューナー監視スクリプト
TSストリームの受信可否およびパケットドロップをcronスケジュール（例: 毎時10分・40分）で並列検査する
"""
import argparse
import concurrent.futures
import datetime
import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

SERVICES = {
    "MX1": "3239123608",
    "CX": "3274001056",
    "TBS": "3273901048",
    "TX": "3274201072",
    "EX": "3274101064",
    "NTV": "3273801040",
    "NHKE": "3273701032",
    "NHKG": "3273601024",
}


def get_tuners(tuner_url: str) -> list:
    """チューナー一覧を取得する"""
    url = f"{tuner_url}/api/tuners"
    req = urllib.request.Request(url, headers={"User-Agent": "watchable-py"})
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            data = res.read().decode("utf-8")
            return json.loads(data)
    except Exception as e:
        print(f"{tuner_url} がおかしい: {e}", file=sys.stderr)
        return None


def check_ts_stream(service_name: str, service_id: str, tuner_url: str) -> bool:
    """
    指定したサービスのストリームを取得し、JSONエラーやTSドロップを検証する
    """
    name = f"{service_name} {service_id}"
    stream_url = f"{tuner_url}/api/services/{service_id}/stream"

    # 1. 最初のデータ取得・JSONエラーチェック
    req = urllib.request.Request(stream_url, headers={"User-Agent": "watchable-py"})
    try:
        resp = urllib.request.urlopen(req, timeout=30)
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="ignore")
            print(f"{name} Error: HTTP {e.code} - {body}")
        except Exception:
            print(f"{name} Error: HTTP {e.code}")
        return False
    except Exception as e:
        print(f"{name} Error: 接続失敗 - {e}")
        return False

    with resp:
        # 最初の1000バイトを取得して検証
        try:
            initial_chunk = resp.read(1000)
        except Exception as e:
            print(f"{name} Error: 読み込み失敗 - {e}")
            return False

        if not initial_chunk:
            print(f"{name} Error: 空っぽ")
            return False

        # JSON判定 (エラーレスポンスがJSONで返るケース)
        try:
            text = initial_chunk.decode("utf-8").strip()
            if text:
                parsed = json.loads(text)
                print(f"{name} Error: JSONなのでだめ {parsed}")
                return False
        except (UnicodeDecodeError, json.JSONDecodeError):
            # バイナリ (TSストリーム) であればデコード失敗またはJSONデコード失敗となるのが正常
            pass

    # 2. ドロップチェック
    # 切り替え時のドロップを回避するため1秒読み捨て、その後の5秒間を tsselect にパイプ
    try:
        resp = urllib.request.urlopen(req, timeout=30)
    except Exception as e:
        print(f"{name} Error: ストリーム再接続失敗 - {e}")
        return False

    with resp:
        # 1秒間読み捨て
        discard_start = time.time()
        while time.time() - discard_start < 1.0:
            try:
                _ = resp.read(32768)
            except Exception:
                break

        # 5秒間 tsselect に流し込む
        try:
            proc = subprocess.Popen(
                ["tsselect", "-"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError:
            print(f"{name} Error: tsselect コマンドが見つかりません")
            return False

        stdout_bytes = b""
        try:
            stream_start = time.time()
            while time.time() - stream_start < 5.0:
                chunk = resp.read(32768)
                if not chunk:
                    break
                try:
                    proc.stdin.write(chunk)
                    proc.stdin.flush()
                except (BrokenPipeError, OSError):
                    break
        except Exception as e:
            print(f"{name} Error: ストリーム転送中にエラー - {e}")
            proc.kill()
            return False
        finally:
            try:
                proc.stdin.close()
            except Exception:
                pass

        try:
            stdout_bytes = proc.stdout.read()
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
            try:
                stdout_bytes = proc.stdout.read()
            except Exception:
                pass
        finally:
            try:
                proc.stdout.close()
            except Exception:
                pass

        stdout = stdout_bytes.decode("utf-8", errors="ignore")

        if proc.returncode != 0 or not stdout.strip():
            print(f"{name} Error: TSじゃない")
            return False

        # 出力内容のドロップ確認 (grep -v "d=  0" 相当)
        lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        if not lines:
            print(f"{name} Error: TSじゃない")
            return False

        has_drop = False
        for line in lines:
            # "d=  0" 以外のドロップを検出 (例: d=  1, d=10 など)
            match = re.search(r"(?:^|[\s,])d=\s*(\d+)", line)
            if match:
                drop_count = int(match.group(1))
                if drop_count > 0:
                    has_drop = True
                    break
            elif re.search(r"(?:^|[\s,])d=", line) and "d=  0" not in line:
                has_drop = True
                break

        if has_drop:
            print(f"{name} Error: dropあり")
            return False

        return True


def run_check(args) -> int:
    """
    1回分の監視処理を実行する。
    戻り値:
      0: 正常終了またはスキップ
      1: エラー発生（リトライ上限到達、チューナー使用中など）
      2: 再起動要求（定期再起動時刻 or 全リトライ失敗 かつ 未使用状態）
    """
    current_hour = datetime.datetime.now().hour

    # 0:00～6:00の間は放送休止があって408になるので処理をスキップ
    if not args.force and current_hour != args.restart_hour and 0 <= current_hour < 6:
        print(f"放送休止のため0～6時はチェックしない（現在 {current_hour} 時）")
        return 0

    tuner_url = os.environ.get("TUNER_URL", "").rstrip("/")
    if not tuner_url:
        print("TUNER_URL 環境変数が設定されていません", file=sys.stderr)
        return 1

    tuners = get_tuners(tuner_url)
    if tuners is None:
        return 1

    free_tuners = [t for t in tuners if t.get("isFree") is True]
    if len(free_tuners) == 0:
        print("あきなし、チェックしない")
        return 0

    if current_hour == args.restart_hour:
        print("再起動時間なので、可能であれば再起動します...")
        tuners = get_tuners(tuner_url)
        if tuners is not None and all(t.get("isUsing") is False for t in tuners):
            return 2
        else:
            print("使われててだめだった")
            return 1

    max_p = len(free_tuners)
    max_retries = 3

    for retry_count in range(1, max_retries + 1):
        failure_occurred = False

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_p) as executor:
            futures = {
                executor.submit(
                    check_ts_stream, name, service_id, tuner_url
                ): (name, service_id)
                for name, service_id in SERVICES.items()
            }

            for future in concurrent.futures.as_completed(futures):
                try:
                    success = future.result()
                    if not success:
                        failure_occurred = True
                except Exception as e:
                    print(f"チェック処理中にエラーが発生しました: {e}")
                    failure_occurred = True

        if failure_occurred:
            if retry_count < max_retries:
                print(f"失敗あり。リトライ中... ({retry_count}/{max_retries})")
                time.sleep(2)
            else:
                print(f"失敗あり。リトライ中... ({retry_count}/{max_retries})")
        else:
            print("全てのサービスが正常に処理されました。")
            return 0

    print("全てのリトライが失敗しました。可能であれば再起動します...")
    tuners = get_tuners(tuner_url)
    if tuners is not None and all(t.get("isUsing") is False for t in tuners):
        return 2
    else:
        print("使われててだめだった")
        return 1


def parse_minutes(cron_minutes_str: str) -> list[int]:
    """カンマ区切りの文字列をソートされた0-59の整数リストに変換する"""
    minutes = set()
    for part in cron_minutes_str.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            val = int(part)
            if 0 <= val < 60:
                minutes.add(val)
        except ValueError:
            pass
    if not minutes:
        return [10, 40]
    return sorted(list(minutes))


def get_next_run_time(now: datetime.datetime, target_minutes: list[int]) -> datetime.datetime:
    """現在時刻から次の実行予定時刻を計算する"""
    for m in target_minutes:
        if m > now.minute or (m == now.minute and now.second == 0 and now.microsecond == 0):
            return now.replace(minute=m, second=0, microsecond=0)

    # 次の時間の最小指定分
    next_hour = (now + datetime.timedelta(hours=1)).replace(
        minute=target_minutes[0], second=0, microsecond=0
    )
    return next_hour


def parse_args():
    default_minutes = os.environ.get("CRON_MINUTES", "10,40")
    parser = argparse.ArgumentParser(description="Watchable stream monitor")
    parser.add_argument(
        "-f", "--force", action="store_true", help="強制実行（深夜スキップを無視）"
    )
    parser.add_argument(
        "-r",
        "--restart-hour",
        type=int,
        default=5,
        help="定期再起動を行う時刻（時）(デフォルト: 5)",
    )
    parser.add_argument(
        "-m",
        "--minutes",
        type=str,
        default=default_minutes,
        help=f"毎時の実行対象分（カンマ区切り、デフォルト: {default_minutes}）",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="1回のみ実行して終了",
    )
    parser.add_argument(
        "--no-initial-run",
        action="store_true",
        help="起動時の即時実行をスキップし、次の指定時刻まで待機する",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    if args.once:
        code = run_check(args)
        sys.exit(code)

    target_minutes = parse_minutes(args.minutes)
    mins_display = ", ".join(f"{m:02d}分" for m in target_minutes)
    print(f"watchable デーモンを開始しました (毎時 {mins_display} に実行)")

    running = True

    def sig_handler(signum, frame):
        nonlocal running
        print(f"\nシグナル ({signum}) を受信しました。停止します...")
        running = False

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    # 起動時初回実行
    if not args.no_initial_run:
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{now_str}] 起動時チェック実行")
        code = run_check(args)
        if code == 2:
            print("再起動が要求されたためプロセスを終了します (exit 2)")
            sys.exit(2)

    while running:
        now = datetime.datetime.now()
        next_run = get_next_run_time(now, target_minutes)
        print(f"次回実行予定: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")

        # 次回実行時刻まで待機
        while running:
            now = datetime.datetime.now()
            remaining = (next_run - now).total_seconds()
            if remaining <= 0:
                break
            # 最大1秒ずつスリープしてシグナル停止に対応
            time.sleep(min(1.0, remaining))

        if not running:
            break

        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{now_str}] 定期監視チェック実行")
        code = run_check(args)

        if code == 2:
            print("再起動が要求されたためプロセスを終了します (exit 2)")
            sys.exit(2)

        # 実行直後、同じ分内で二重実行されないよう最低1秒スリープ
        time.sleep(1)

    print("watchable デーモンを終了しました。")
    sys.exit(0)


if __name__ == "__main__":
    main()
