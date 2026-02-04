"""Tests for parsing BLS release schedule pages."""

from src.analytics.bls_release_schedule import extract_tables, parse_schedule_html


def test_extract_tables_finds_table_cells():
    html = """
    <html><body>
      <table>
        <tr><th>Release Date</th><th>Release Time</th></tr>
        <tr><td>February 10, 2026</td><td>8:30 a.m. (ET)</td></tr>
      </table>
    </body></html>
    """
    tables = extract_tables(html)
    assert len(tables) == 1
    assert tables[0][0] == ["Release Date", "Release Time"]
    assert tables[0][1] == ["February 10, 2026", "8:30 a.m. (ET)"]


def test_parse_schedule_html_parses_release_dates_and_times_to_utc():
    html = """
    <html>
      <head><title>Schedule</title></head>
      <body>
        <h1>Consumer Price Index</h1>
        <table class="regular">
          <tr><th>Release Date</th><th>Release Time</th><th>Reference Month</th></tr>
          <tr><td>February 10, 2026</td><td>8:30 a.m. (ET)</td><td>January 2026</td></tr>
          <tr><td>March 12, 2026</td><td>8:30 a.m. (ET)</td><td>February 2026</td></tr>
        </table>
      </body>
    </html>
    """
    events = parse_schedule_html(
        html,
        series_id="cu",
        release="Consumer Price Index",
        url="https://www.bls.gov/schedule/news_release/cpi.htm",
    )
    assert len(events) == 2
    assert events[0]["series"] == "cu"
    assert events[0]["release"] == "Consumer Price Index"
    assert events[0]["scheduled_time"] == "2026-02-10T13:30:00Z"  # 8:30 ET in winter
    assert events[1]["scheduled_time"] == "2026-03-12T12:30:00Z"  # 8:30 ET in DST (EDT)
