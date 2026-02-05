"""Tests for aws_observability exports."""

from __future__ import annotations

from datetime import datetime, timezone

from src.analytics import aws_observability


class StubCloudWatch:
    def __init__(self, *, datapoints_by_key: dict[tuple, list[dict]]):
        self._data = datapoints_by_key

    def get_metric_statistics(
        self,
        *,
        Namespace,
        MetricName,
        Dimensions,
        StartTime,
        EndTime,
        Period,
        Statistics,
    ):
        dims = tuple((d.get("Name"), d.get("Value")) for d in (Dimensions or []))
        stat = Statistics[0] if Statistics else None
        key = (Namespace, MetricName, dims, stat)
        return {"Datapoints": self._data.get(key, [])}


class StubCostExplorer:
    def __init__(self, *, actual_by_date: dict[str, str], forecast_by_date: dict[str, dict[str, str]]):
        self._actual = actual_by_date
        self._forecast = forecast_by_date

    def get_cost_and_usage(self, *, TimePeriod, Granularity, Metrics, Filter=None):
        metric = Metrics[0]
        out = []
        # Return only days explicitly provided (cost explorer would return the full range).
        for start in sorted(self._actual.keys()):
            out.append({
                "TimePeriod": {"Start": start, "End": start},
                "Total": {metric: {"Amount": self._actual[start], "Unit": "USD"}},
            })
        return {"ResultsByTime": out}

    def get_cost_forecast(self, *, TimePeriod, Granularity, Metric, Filter=None):
        out = []
        for start in sorted(self._forecast.keys()):
            row = self._forecast[start]
            out.append({
                "TimePeriod": {"Start": start, "End": start},
                "MeanValue": row.get("mean"),
                "PredictionIntervalLowerBound": row.get("lower"),
                "PredictionIntervalUpperBound": row.get("upper"),
            })
        return {"Total": {"Unit": "USD"}, "ForecastResultsByTime": out}


def test_build_payload_includes_metrics_and_cost(monkeypatch):
    now = datetime(2026, 2, 4, 12, 0, tzinfo=timezone.utc)

    cw_key_invocations = (
        "AWS/Lambda",
        "Invocations",
        (("FunctionName", "fomc-data-fetcher"),),
        "Sum",
    )
    cw_key_sqs_sent = (
        "AWS/SQS",
        "NumberOfMessagesSent",
        (("QueueName", "fomc-analytics-queue"),),
        "Sum",
    )

    cw = StubCloudWatch(datapoints_by_key={
        cw_key_invocations: [{"Timestamp": datetime(2026, 2, 3, 0, 0, tzinfo=timezone.utc), "Sum": 5}],
        cw_key_sqs_sent: [{"Timestamp": datetime(2026, 2, 4, 0, 0, tzinfo=timezone.utc), "Sum": 2}],
    })

    ce = StubCostExplorer(
        actual_by_date={
            "2026-02-02": "0.10",
            "2026-02-03": "0.20",
            "2026-02-04": "0.15",
        },
        forecast_by_date={
            "2026-02-04": {"mean": "0.16", "lower": "0.12", "upper": "0.20"},
            "2026-02-05": {"mean": "0.17", "lower": "0.13", "upper": "0.21"},
        },
    )

    def fake_get_client(service: str):
        if service == "cloudwatch":
            return cw
        if service == "ce":
            return ce
        raise ValueError(service)

    monkeypatch.setattr(aws_observability, "get_client", fake_get_client)

    payload = aws_observability.build_aws_observability_payload(
        now=now,
        window_days=3,
        forecast_days=2,
        lambda_functions=["fomc-data-fetcher"],
        sqs_queues=["fomc-analytics-queue"],
        include_s3_storage_metrics=False,
        include_cost=True,
    )

    assert payload["metric_dates"] == ["2026-02-02", "2026-02-03", "2026-02-04"]
    assert payload["errors"] == []

    series = payload["metrics"]["series"]
    assert len(series) == len(aws_observability.LAMBDA_METRICS) + len(aws_observability.SQS_METRICS)

    inv = next(s for s in series if s["id"] == "lambda.fomc-data-fetcher.Invocations.Sum")
    assert inv["values"] == [None, 5.0, None]

    sent = next(s for s in series if s["id"] == "sqs.fomc-analytics-queue.NumberOfMessagesSent.Sum")
    assert sent["values"] == [None, None, 2.0]

    cost = payload["cost"]
    assert cost["currency"] == "USD"
    assert cost["dates"] == ["2026-02-02", "2026-02-03", "2026-02-04", "2026-02-05"]
    assert cost["actual"] == [0.10, 0.20, 0.15, None]
    assert cost["predicted"] == [None, None, 0.16, 0.17]
    assert cost["predicted_lower"] == [None, None, 0.12, 0.13]
    assert cost["predicted_upper"] == [None, None, 0.20, 0.21]

