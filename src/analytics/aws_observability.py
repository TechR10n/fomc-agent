"""AWS observability exports for the static dashboard.

This module is intentionally lightweight: it queries CloudWatch for a handful
of key service metrics (Lambda, SQS, optionally S3 storage) and AWS Cost
Explorer for actuals + forecast. The output is a compact JSON payload the
static site can render client-side.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal

from src.config import (
    get_analytics_dlq_name,
    get_analytics_queue_name,
    get_bls_bucket,
    get_bucket_prefix,
    get_datausa_bucket,
)
from src.helpers.aws_client import get_client

CloudWatchStat = Literal["Average", "Sum", "Minimum", "Maximum", "SampleCount"]


@dataclass(frozen=True)
class MetricDef:
    namespace: str
    metric_name: str
    stat: CloudWatchStat
    unit: str
    label: str


DEFAULT_LAMBDA_FUNCTIONS = ["fomc-data-fetcher", "fomc-analytics-processor"]


def _default_sqs_queues() -> list[str]:
    return [get_analytics_queue_name(), get_analytics_dlq_name()]


LAMBDA_METRICS: list[MetricDef] = [
    MetricDef("AWS/Lambda", "Invocations", "Sum", "Count", "Invocations"),
    MetricDef("AWS/Lambda", "Errors", "Sum", "Count", "Errors"),
    MetricDef("AWS/Lambda", "Throttles", "Sum", "Count", "Throttles"),
    MetricDef("AWS/Lambda", "Duration", "Average", "Milliseconds", "Duration (avg ms)"),
    MetricDef("AWS/Lambda", "Duration", "Sum", "Milliseconds", "Duration (sum ms)"),
    MetricDef("AWS/Lambda", "ConcurrentExecutions", "Maximum", "Count", "Concurrent executions (max)"),
]

SQS_METRICS: list[MetricDef] = [
    MetricDef("AWS/SQS", "ApproximateNumberOfMessagesVisible", "Average", "Count", "Visible messages (avg)"),
    MetricDef("AWS/SQS", "ApproximateNumberOfMessagesNotVisible", "Average", "Count", "Not visible messages (avg)"),
    MetricDef("AWS/SQS", "ApproximateAgeOfOldestMessage", "Maximum", "Seconds", "Oldest message (max s)"),
    MetricDef("AWS/SQS", "NumberOfMessagesSent", "Sum", "Count", "Messages sent"),
    MetricDef("AWS/SQS", "NumberOfMessagesReceived", "Sum", "Count", "Messages received"),
    MetricDef("AWS/SQS", "NumberOfMessagesDeleted", "Sum", "Count", "Messages deleted"),
    MetricDef("AWS/SQS", "NumberOfEmptyReceives", "Sum", "Count", "Empty receives"),
    MetricDef("AWS/SQS", "SentMessageSize", "Average", "Bytes", "Sent message size (avg bytes)"),
]


def _parse_csv(value: str | None) -> list[str] | None:
    if not value:
        return None
    parts = [p.strip() for p in str(value).split(",")]
    out = [p for p in parts if p]
    return out or None


def _utc_now(now: datetime | None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=timezone.utc)
    return now.astimezone(timezone.utc)


def _date_keys(*, end: date, days: int) -> list[str]:
    if days <= 0:
        days = 30
    start = end - timedelta(days=days - 1)
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


def _cw_dimensions(service: Literal["lambda", "sqs"], resource_name: str) -> list[dict[str, str]]:
    if service == "lambda":
        return [{"Name": "FunctionName", "Value": resource_name}]
    if service == "sqs":
        return [{"Name": "QueueName", "Value": resource_name}]
    raise ValueError(f"Unknown service: {service}")


def _align_values(dates: list[str], values_by_date: dict[str, float]) -> list[float | None]:
    return [values_by_date.get(d) for d in dates]


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fetch_cloudwatch_series(
    cloudwatch_client,
    *,
    metric: MetricDef,
    dimensions: list[dict[str, str]],
    start_time: datetime,
    end_time: datetime,
    period_seconds: int = 86400,
) -> dict[str, float]:
    """Fetch daily CloudWatch metric statistics and return date->value."""
    resp = cloudwatch_client.get_metric_statistics(
        Namespace=metric.namespace,
        MetricName=metric.metric_name,
        Dimensions=dimensions,
        StartTime=_ensure_utc(start_time),
        EndTime=_ensure_utc(end_time),
        Period=int(period_seconds),
        Statistics=[metric.stat],
    )

    values: dict[str, float] = {}
    for dp in resp.get("Datapoints", []):
        ts = dp.get("Timestamp")
        if not isinstance(ts, datetime):
            continue
        ts_utc = _ensure_utc(ts)
        key = ts_utc.date().isoformat()
        raw = dp.get(metric.stat)
        if raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue

        # If multiple points land on the same date, prefer additive for "Sum".
        if key in values and metric.stat == "Sum":
            values[key] += value
        else:
            values[key] = value

    return values


def _build_cost_filter(
    *,
    tag_key: str | None,
    tag_values: list[str] | None,
    services: list[str] | None,
) -> dict[str, Any] | None:
    parts: list[dict[str, Any]] = []

    if tag_key and tag_values:
        parts.append({
            "Tags": {
                "Key": tag_key,
                "Values": tag_values,
                "MatchOptions": ["EQUALS"],
            }
        })

    if services:
        parts.append({
            "Dimensions": {
                "Key": "SERVICE",
                "Values": services,
            }
        })

    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return {"And": parts}


def fetch_cost_actual(
    ce_client,
    *,
    start_date: str,
    end_date_exclusive: str,
    cost_filter: dict[str, Any] | None,
    metric_name: str = "UnblendedCost",
    granularity: Literal["DAILY", "MONTHLY"] = "DAILY",
) -> tuple[dict[str, float], str | None]:
    req: dict[str, Any] = {
        "TimePeriod": {"Start": start_date, "End": end_date_exclusive},
        "Granularity": granularity,
        "Metrics": [metric_name],
    }
    if cost_filter is not None:
        req["Filter"] = cost_filter

    resp = ce_client.get_cost_and_usage(**req)

    currency = None
    values: dict[str, float] = {}
    for item in resp.get("ResultsByTime", []):
        period = item.get("TimePeriod", {})
        start = period.get("Start")
        total = item.get("Total", {}).get(metric_name, {})
        amount = total.get("Amount")
        unit = total.get("Unit")
        if currency is None and isinstance(unit, str):
            currency = unit
        if not start or amount is None:
            continue
        try:
            values[str(start)] = float(amount)
        except (TypeError, ValueError):
            continue
    return values, currency


def fetch_cost_forecast(
    ce_client,
    *,
    start_date: str,
    end_date_exclusive: str,
    cost_filter: dict[str, Any] | None,
    metric: Literal[
        "BLENDED_COST",
        "UNBLENDED_COST",
        "AMORTIZED_COST",
        "NET_UNBLENDED_COST",
        "NET_AMORTIZED_COST",
    ] = "UNBLENDED_COST",
    granularity: Literal["DAILY", "MONTHLY"] = "DAILY",
) -> tuple[dict[str, dict[str, float]], str | None]:
    req: dict[str, Any] = {
        "TimePeriod": {"Start": start_date, "End": end_date_exclusive},
        "Granularity": granularity,
        "Metric": metric,
    }
    if cost_filter is not None:
        req["Filter"] = cost_filter

    resp = ce_client.get_cost_forecast(**req)

    currency = None
    total = resp.get("Total", {})
    unit = total.get("Unit")
    if isinstance(unit, str):
        currency = unit

    out: dict[str, dict[str, float]] = {}
    for item in resp.get("ForecastResultsByTime", []):
        period = item.get("TimePeriod", {})
        start = period.get("Start")
        if not start:
            continue

        row: dict[str, float] = {}
        for k_src, k_dst in [
            ("MeanValue", "mean"),
            ("PredictionIntervalLowerBound", "lower"),
            ("PredictionIntervalUpperBound", "upper"),
        ]:
            raw = item.get(k_src)
            if raw is None:
                continue
            try:
                row[k_dst] = float(raw)
            except (TypeError, ValueError):
                continue

        if row:
            out[str(start)] = row
    return out, currency


def build_aws_observability_payload(
    *,
    now: datetime | None = None,
    window_days: int = 30,
    forecast_days: int = 30,
    lambda_functions: list[str] | None = None,
    sqs_queues: list[str] | None = None,
    include_s3_storage_metrics: bool = True,
    include_cost: bool = True,
) -> dict[str, Any]:
    now_utc = _utc_now(now)
    end_date = now_utc.date()
    metric_dates = _date_keys(end=end_date, days=window_days)

    if lambda_functions is None:
        lambda_functions = _parse_csv(os.environ.get("FOMC_OBS_LAMBDA_FUNCTIONS")) or DEFAULT_LAMBDA_FUNCTIONS
    if sqs_queues is None:
        sqs_queues = _parse_csv(os.environ.get("FOMC_OBS_SQS_QUEUES")) or _default_sqs_queues()

    bls_bucket = get_bls_bucket()
    datausa_bucket = get_datausa_bucket()
    prefix = get_bucket_prefix()

    errors: list[dict[str, Any]] = []

    cloudwatch = None
    try:
        cloudwatch = get_client("cloudwatch")
    except Exception as e:
        errors.append({"source": "cloudwatch", "error": str(e)})

    start_time = datetime.combine(end_date - timedelta(days=window_days - 1), datetime.min.time(), tzinfo=timezone.utc)
    end_time = datetime.combine(end_date + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)

    series: list[dict[str, Any]] = []
    if cloudwatch is not None:
        for fn in lambda_functions:
            for metric in LAMBDA_METRICS:
                try:
                    values = fetch_cloudwatch_series(
                        cloudwatch,
                        metric=metric,
                        dimensions=_cw_dimensions("lambda", fn),
                        start_time=start_time,
                        end_time=end_time,
                        period_seconds=86400,
                    )
                except Exception as e:
                    errors.append({
                        "source": "cloudwatch",
                        "service": "lambda",
                        "resource": fn,
                        "metric": metric.metric_name,
                        "stat": metric.stat,
                        "error": str(e),
                    })
                    values = {}

                series.append({
                    "id": f"lambda.{fn}.{metric.metric_name}.{metric.stat}",
                    "group": f"Lambda / {fn}",
                    "service": "lambda",
                    "resource": fn,
                    "metric": metric.metric_name,
                    "stat": metric.stat,
                    "unit": metric.unit,
                    "label": metric.label,
                    "values": _align_values(metric_dates, values),
                })

        for q in sqs_queues:
            for metric in SQS_METRICS:
                try:
                    values = fetch_cloudwatch_series(
                        cloudwatch,
                        metric=metric,
                        dimensions=_cw_dimensions("sqs", q),
                        start_time=start_time,
                        end_time=end_time,
                        period_seconds=86400,
                    )
                except Exception as e:
                    errors.append({
                        "source": "cloudwatch",
                        "service": "sqs",
                        "resource": q,
                        "metric": metric.metric_name,
                        "stat": metric.stat,
                        "error": str(e),
                    })
                    values = {}

                series.append({
                    "id": f"sqs.{q}.{metric.metric_name}.{metric.stat}",
                    "group": f"SQS / {q}",
                    "service": "sqs",
                    "resource": q,
                    "metric": metric.metric_name,
                    "stat": metric.stat,
                    "unit": metric.unit,
                    "label": metric.label,
                    "values": _align_values(metric_dates, values),
                })

        if include_s3_storage_metrics:
            # S3 storage metrics are slow-moving (daily) but useful for "other AWS metrics".
            s3_defs = [
                ("NumberOfObjects", "Average", "Count", "Objects (avg)", "AllStorageTypes"),
                ("BucketSizeBytes", "Average", "Bytes", "Bucket size (avg bytes)", "StandardStorage"),
            ]
            for bucket in [bls_bucket, datausa_bucket, f"{prefix}-site"]:
                for metric_name, stat, unit, label, storage_type in s3_defs:
                    metric = MetricDef("AWS/S3", metric_name, stat, unit, label)  # type: ignore[arg-type]
                    try:
                        values = fetch_cloudwatch_series(
                            cloudwatch,
                            metric=metric,
                            dimensions=[
                                {"Name": "BucketName", "Value": bucket},
                                {"Name": "StorageType", "Value": storage_type},
                            ],
                            start_time=start_time,
                            end_time=end_time,
                            period_seconds=86400,
                        )
                    except Exception as e:
                        errors.append({
                            "source": "cloudwatch",
                            "service": "s3",
                            "resource": bucket,
                            "metric": metric_name,
                            "stat": stat,
                            "error": str(e),
                        })
                        values = {}

                    series.append({
                        "id": f"s3.{bucket}.{metric_name}.{stat}.{storage_type}",
                        "group": f"S3 / {bucket}",
                        "service": "s3",
                        "resource": bucket,
                        "metric": metric_name,
                        "stat": stat,
                        "unit": unit,
                        "label": f"{label} ({storage_type})",
                        "values": _align_values(metric_dates, values),
                    })

    cost: dict[str, Any] = {"currency": None, "dates": [], "actual": [], "predicted": [], "predicted_lower": [], "predicted_upper": []}

    if include_cost:
        ce = None
        try:
            ce = get_client("ce")
        except Exception as e:
            errors.append({"source": "cost-explorer", "error": str(e)})

        if ce is not None:
            tag_key = os.environ.get("FOMC_COST_TAG_KEY")
            tag_values = _parse_csv(os.environ.get("FOMC_COST_TAG_VALUES") or os.environ.get("FOMC_COST_TAG_VALUE"))
            services = _parse_csv(
                os.environ.get("FOMC_COST_SERVICES")
                or "AWS Lambda,Amazon Simple Queue Service,Amazon Simple Storage Service"
            )
            cost_filter = _build_cost_filter(tag_key=tag_key, tag_values=tag_values, services=services)

            actual_start = (end_date - timedelta(days=window_days - 1)).isoformat()
            actual_end_excl = (end_date + timedelta(days=1)).isoformat()
            forecast_start = end_date.isoformat()
            forecast_end_excl = (end_date + timedelta(days=max(0, forecast_days))).isoformat()

            actual_by_date: dict[str, float] = {}
            forecast_by_date: dict[str, dict[str, float]] = {}
            currency = None
            try:
                actual_by_date, currency = fetch_cost_actual(
                    ce,
                    start_date=actual_start,
                    end_date_exclusive=actual_end_excl,
                    cost_filter=cost_filter,
                )
            except Exception as e:
                errors.append({"source": "cost-explorer", "kind": "actual", "error": str(e)})

            forecast_currency = None
            if forecast_days > 0:
                try:
                    forecast_by_date, forecast_currency = fetch_cost_forecast(
                        ce,
                        start_date=forecast_start,
                        end_date_exclusive=forecast_end_excl,
                        cost_filter=cost_filter,
                    )
                except Exception as e:
                    errors.append({"source": "cost-explorer", "kind": "forecast", "error": str(e)})

            if currency is None:
                currency = forecast_currency

            total_days = window_days + max(0, forecast_days)
            # Forecast starts on `end_date`, which overlaps with the last day of the actual window.
            if forecast_days > 0:
                total_days -= 1
            end_cost_date = end_date + timedelta(days=max(forecast_days - 1, 0))
            cost_dates = _date_keys(end=end_cost_date, days=total_days)

            cost = {
                "currency": currency,
                "dates": cost_dates,
                "actual": [actual_by_date.get(d) for d in cost_dates],
                "predicted": [forecast_by_date.get(d, {}).get("mean") for d in cost_dates],
                "predicted_lower": [forecast_by_date.get(d, {}).get("lower") for d in cost_dates],
                "predicted_upper": [forecast_by_date.get(d, {}).get("upper") for d in cost_dates],
                "filter": {
                    "tag_key": tag_key,
                    "tag_values": tag_values,
                    "services": services,
                },
            }

    return {
        "generated_at": now_utc.isoformat().replace("+00:00", "Z"),
        "window_days": int(window_days),
        "forecast_days": int(max(0, forecast_days)),
        "granularity": "DAILY",
        "metric_dates": metric_dates,
        "resources": {
            "lambda_functions": lambda_functions,
            "sqs_queues": sqs_queues,
            "s3_buckets": [bls_bucket, datausa_bucket, f"{prefix}-site"],
        },
        "metrics": {"series": series},
        "cost": cost,
        "errors": errors,
    }


def export_aws_observability(
    *,
    out_path: str | Path = Path("site/data/aws_observability.json"),
    now: datetime | None = None,
    window_days: int = 30,
    forecast_days: int = 30,
) -> Path:
    payload = build_aws_observability_payload(
        now=now,
        window_days=window_days,
        forecast_days=forecast_days,
    )
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    return path.resolve()
