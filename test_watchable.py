import os
import unittest
from unittest.mock import patch, MagicMock
import re
import datetime

import watchable


class TestWatchable(unittest.TestCase):
    def test_parse_args(self):
        with patch("sys.argv", ["watchable.py", "-f", "-r", "4", "-m", "10,40", "--once"]):
            args = watchable.parse_args()
            self.assertTrue(args.force)
            self.assertEqual(args.restart_hour, 4)
            self.assertEqual(args.minutes, "10,40")
            self.assertTrue(args.once)

    def test_parse_minutes(self):
        self.assertEqual(watchable.parse_minutes("10,40"), [10, 40])
        self.assertEqual(watchable.parse_minutes("40, 10, 10"), [10, 40])
        self.assertEqual(watchable.parse_minutes("0,15,30,45"), [0, 15, 30, 45])
        self.assertEqual(watchable.parse_minutes("invalid, 99, -5"), [10, 40])

    def test_get_next_run_time(self):
        # 18:05 -> 18:10:00
        now = datetime.datetime(2026, 8, 9, 18, 5, 23)
        next_run = watchable.get_next_run_time(now, [10, 40])
        self.assertEqual(next_run, datetime.datetime(2026, 8, 9, 18, 10, 0))

        # 18:10:01 -> 18:40:00
        now = datetime.datetime(2026, 8, 9, 18, 10, 1)
        next_run = watchable.get_next_run_time(now, [10, 40])
        self.assertEqual(next_run, datetime.datetime(2026, 8, 9, 18, 40, 0))

        # 18:45:00 -> 19:10:00
        now = datetime.datetime(2026, 8, 9, 18, 45, 0)
        next_run = watchable.get_next_run_time(now, [10, 40])
        self.assertEqual(next_run, datetime.datetime(2026, 8, 9, 19, 10, 0))

        # 23:55:00 -> 翌日 00:10:00
        now = datetime.datetime(2026, 8, 9, 23, 55, 0)
        next_run = watchable.get_next_run_time(now, [10, 40])
        self.assertEqual(next_run, datetime.datetime(2026, 8, 10, 0, 10, 0))

    def test_drop_detection_logic(self):
        stdout_ok = """pid=0x0000, total=    123, d=  0, error=  0
pid=0x0100, total=   4567, d=  0, error=  0"""

        stdout_drop = """pid=0x0000, total=    123, d=  0, error=  0
pid=0x0100, total=   4567, d=  1, error=  0"""

        def check_output(stdout_str):
            lines = [line.strip() for line in stdout_str.splitlines() if line.strip()]
            for line in lines:
                match = re.search(r"(?:^|[\s,])d=\s*(\d+)", line)
                if match:
                    if int(match.group(1)) > 0:
                        return True
                elif re.search(r"(?:^|[\s,])d=", line) and "d=  0" not in line:
                    return True
            return False

        self.assertFalse(check_output(stdout_ok))
        self.assertTrue(check_output(stdout_drop))

    @patch("watchable.urllib.request.urlopen")
    def test_get_tuners_success(self, mock_urlopen):
        mock_res = MagicMock()
        mock_res.read.return_value = (
            b'[{"name": "tuner0", "isFree": true, "isUsing": false}]'
        )
        mock_urlopen.return_value.__enter__.return_value = mock_res

        tuners = watchable.get_tuners("http://localhost:40772")
        self.assertIsNotNone(tuners)
        self.assertEqual(len(tuners), 1)
        self.assertTrue(tuners[0]["isFree"])
        self.assertFalse(tuners[0]["isUsing"])

    @patch("watchable.urllib.request.urlopen")
    def test_get_tuners_failure(self, mock_urlopen):
        mock_urlopen.side_effect = Exception("Connection refused")
        tuners = watchable.get_tuners("http://localhost:40772")
        self.assertIsNone(tuners)

    @patch("watchable.datetime")
    def test_run_check_night_skip(self, mock_dt):
        mock_dt.datetime.now.return_value = datetime.datetime(2026, 8, 9, 3, 0, 0)
        args = MagicMock()
        args.force = False
        args.restart_hour = 5
        code = watchable.run_check(args)
        self.assertEqual(code, 0)

    @patch("watchable.datetime")
    @patch.dict(os.environ, {"TUNER_URL": "http://localhost:40772"})
    @patch("watchable.get_tuners")
    def test_run_check_no_free_tuners(self, mock_get_tuners, mock_dt):
        mock_dt.datetime.now.return_value = datetime.datetime(2026, 8, 9, 12, 0, 0)
        mock_get_tuners.return_value = [
            {"name": "tuner0", "isFree": False, "isUsing": True}
        ]
        args = MagicMock()
        args.force = True
        args.restart_hour = 5
        code = watchable.run_check(args)
        self.assertEqual(code, 0)

    @patch("watchable.datetime")
    @patch.dict(os.environ, {"TUNER_URL": "http://localhost:40772"})
    @patch("watchable.get_tuners")
    def test_run_check_restart_hour_code_2(self, mock_get_tuners, mock_dt):
        mock_dt.datetime.now.return_value = datetime.datetime(2026, 8, 9, 5, 0, 0)
        mock_get_tuners.return_value = [
            {"name": "tuner0", "isFree": True, "isUsing": False}
        ]
        args = MagicMock()
        args.force = False
        args.restart_hour = 5
        code = watchable.run_check(args)
        self.assertEqual(code, 2)

    @patch("watchable.datetime")
    @patch.dict(os.environ, {"TUNER_URL": "http://localhost:40772"})
    @patch("watchable.get_tuners")
    @patch("watchable.check_ts_stream", return_value=False)
    def test_run_check_failure_code_3(self, mock_check_stream, mock_get_tuners, mock_dt):
        # 朝5時以外 (例: 18時) でチェック失敗 -> 未使用なら USB リセット (code 3)
        mock_dt.datetime.now.return_value = datetime.datetime(2026, 8, 9, 18, 0, 0)
        mock_get_tuners.return_value = [
            {"name": "tuner0", "isFree": True, "isUsing": False}
        ]
        args = MagicMock()
        args.force = False
        args.restart_hour = 5
        code = watchable.run_check(args)
        self.assertEqual(code, 3)

    @patch("watchable.run_check", return_value=0)
    @patch("sys.argv", ["watchable.py", "--once"])
    def test_main_once(self, mock_run_check):
        with self.assertRaises(SystemExit) as cm:
            watchable.main()
        self.assertEqual(cm.exception.code, 0)
        mock_run_check.assert_called_once()


if __name__ == "__main__":
    unittest.main()

