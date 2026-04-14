import unittest
from pathlib import Path

from scripts.generate_top10_rank_returns_html import generate_report


class Top10RankReturnsReportTest(unittest.TestCase):
    def test_generate_report_embeds_expected_sections(self) -> None:
        tmp_dir = Path(self._testMethodName)
        tmp_dir.mkdir(exist_ok=True)
        self.addCleanup(lambda: [p.unlink() for p in tmp_dir.iterdir()] or tmp_dir.rmdir())

        csv_path = tmp_dir / "top10.csv"
        html_path = tmp_dir / "report.html"
        csv_path.write_text(
            "\n".join(
                [
                    "ranking_local_date,account_id,rank,symbol,rank_pct_change,run_found,run_id,trade_day_utc,started_at_utc,started_at_local,position_found,position_id,position_status,close_reason,entry_price,entry_fill_avg_price,exit_fill_avg_price,opened_at_utc,closed_at_utc,return_pct,return_basis",
                    "2026-04-10,acc01,1,RAVEUSDT,193.48,1,run-1,2026-04-09,2026-04-09T23:40:13+00:00,2026-04-10T07:40:13+08:00,1,1001,CLOSED_TP,TP,1.60,1.60,1.44,2026-04-09T23:40:13+00:00,2026-04-10T01:10:13+00:00,10.0,entry_to_exit_fill",
                    "2026-04-10,acc01,2,AGTUSDT,89.33,1,run-1,2026-04-09,2026-04-09T23:40:13+00:00,2026-04-10T07:40:13+08:00,0,,,,,,,,,,rank_symbol_not_opened",
                    "2026-04-10,acc02,1,RAVEUSDT,193.48,1,run-2,2026-04-09,2026-04-09T23:45:00+00:00,2026-04-10T07:45:00+08:00,1,1002,OPEN,,1.61,1.61,,2026-04-09T23:45:00+00:00,,,open_position_no_exit_fill",
                ]
            ),
            encoding="utf-8",
        )

        generate_report(csv_path=csv_path, output_path=html_path)

        html = html_path.read_text(encoding="utf-8")
        self.assertIn("涨幅榜做空收益可视化", html)
        self.assertIn("总览仪表板", html)
        self.assertIn("明细穿透", html)
        self.assertIn("acc01", html)
        self.assertIn("RAVEUSDT", html)


if __name__ == "__main__":
    unittest.main()
