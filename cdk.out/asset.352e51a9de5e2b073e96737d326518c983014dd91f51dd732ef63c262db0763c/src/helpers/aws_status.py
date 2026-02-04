"""AWS resource status checks for S3, SQS, and Lambda."""

import json

from src.helpers.aws_client import get_client


def check_s3_status() -> dict:
    """List S3 buckets and object counts."""
    s3 = get_client("s3")
    result = {}
    try:
        buckets = s3.list_buckets().get("Buckets", [])
        for bucket in buckets:
            name = bucket["Name"]
            try:
                objects = s3.list_objects_v2(Bucket=name)
                count = objects.get("KeyCount", 0)
            except Exception:
                count = -1
            result[name] = {"object_count": count}
    except Exception as e:
        result["_error"] = str(e)
    return result


def check_sqs_status() -> dict:
    """List SQS queues and message counts."""
    sqs = get_client("sqs")
    result = {}
    try:
        queues = sqs.list_queues()
        queue_urls = queues.get("QueueUrls", [])
        for url in queue_urls:
            attrs = sqs.get_queue_attributes(
                QueueUrl=url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            name = url.split("/")[-1]
            msg_count = int(
                attrs.get("Attributes", {}).get("ApproximateNumberOfMessages", 0)
            )
            result[name] = {"queue_url": url, "message_count": msg_count}
    except Exception as e:
        result["_error"] = str(e)
    return result


def check_lambda_status() -> dict:
    """List Lambda functions and configs."""
    lam = get_client("lambda")
    result = {}
    try:
        functions = lam.list_functions().get("Functions", [])
        for fn in functions:
            name = fn["FunctionName"]
            result[name] = {
                "runtime": fn.get("Runtime", "N/A"),
                "memory": fn.get("MemorySize", 0),
                "timeout": fn.get("Timeout", 0),
                "last_modified": fn.get("LastModified", "N/A"),
            }
    except Exception as e:
        result["_error"] = str(e)
    return result


def check_all_status() -> dict:
    """Combined status check of all AWS services."""
    return {
        "s3": check_s3_status(),
        "sqs": check_sqs_status(),
        "lambda": check_lambda_status(),
    }


if __name__ == "__main__":
    print(json.dumps(check_all_status(), indent=2, default=str))
