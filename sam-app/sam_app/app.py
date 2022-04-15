import json
import typing
import logging
import random
import base64

import requests
import boto3
from botocore.exceptions import ClientError
import pytz
import datetime


def is_opt_out(
    date: str,
    interval: list,
    local_timezone: str,
    local_start_opt_out: str,
    local_end_opt_out: str,
) -> bool:
    # create start, end datetime objects
    local_tz = pytz.timezone(local_timezone)
    local_start_date_obj: datetime.datetime = local_tz.localize(
        datetime.datetime.strptime(
            date,
            "%y/%m/%d,%H:%M:%S",
        )
    )
    local_end_date_obj: datetime.datetime = local_start_date_obj + datetime.timedelta(
        seconds=sum(interval)
    )

    # create opt out datetime objects
    local_date_obj_start_opt_out = local_start_date_obj.replace(
        hour=int(local_start_opt_out.split(":")[0]),
        minute=int(local_start_opt_out.split(":")[1]),
    )
    local_date_obj_end_opt_out = local_start_date_obj.replace(
        hour=int(local_end_opt_out.split(":")[0]),
        minute=int(local_end_opt_out.split(":")[1]),
    )

    # check if start, end dates are between opt out time
    if (
        local_date_obj_start_opt_out
        <= local_start_date_obj
        <= local_date_obj_end_opt_out
    ) or (
        local_date_obj_start_opt_out <= local_end_date_obj <= local_date_obj_end_opt_out
    ):
        return True
    return False


def convert_date_to_utc(date: str, local_timezone: str) -> str:
    local_tz = pytz.timezone(local_timezone)
    local_date_obj: datetime.datetime = local_tz.localize(
        datetime.datetime.strptime(
            date,
            "%y/%m/%d,%H:%M:%S",
        )
    )
    utc_date_obj = local_date_obj.astimezone(pytz.timezone("UTC"))
    return utc_date_obj.isoformat()


def get_item_from_dynamodb(device_id: str) -> typing.Tuple[str, str, str]:
    dynamodb_client = boto3.client("dynamodb")
    response = dynamodb_client.get_item(
        TableName="test_devices_table", Key={"device_id": {"S": device_id}}
    )
    return (
        response["Item"]["timezone"]["S"],
        response["Item"]["local_start_opt_out"]["S"],
        response["Item"]["local_end_opt_out"]["S"],
    )


def write_file_to_s3(output: dict, token: str) -> None:
    s3_client = boto3.client("s3")
    output_s3 = {"Header": {"Authorization": token}, "Body": output}
    try:
        s3_client.put_object(
            Body=str(json.dumps(output_s3)),
            Bucket="project-intro-bucket",
            Key=f"{output['devId']}.json",
        )
    except ClientError as e:
        logging.error(e)


def slack_message(device_id: str) -> None:
    url: str = "https://hooks.slack.com/services/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"
    try:
        requests.post(
            url,
            headers={"Content-Type": "application/json"},
            json={"text": f"Schedule for device {device_id} was sent"},
        )
    except Exception as e:
        logging.error(e)


def get_secret():

    secret_name = (
        "arn:aws:secretsmanager:us-west-2:261943945236:secret:faketoken-07dc4z"
    )
    region_name = "us-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logging.error(e)
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
            return secret
        else:
            decoded_binary_secret = base64.b64decode(
                get_secret_value_response["SecretBinary"]
            )
            return decoded_binary_secret


def lambda_handler(event, context):

    request_body: dict = json.loads(event["body"])

    (timezone, local_start_opt_out, local_end_opt_out) = get_item_from_dynamodb(
        request_body["devId"]
    )

    if is_opt_out(
        request_body["startAt"],
        request_body["interval"],
        timezone,
        local_start_opt_out,
        local_end_opt_out,
    ):
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "This device is opt out",
                }
            ),
            "headers": {"Content-Type": "application/json"},
        }

    date_utc: str = convert_date_to_utc(request_body["startAt"], timezone)

    # add random number in the last interval element
    random_interval: list = request_body["interval"]
    random_interval[-1] = random_interval[-1] + random.randint(1, 600)

    response_body: dict = {
        "type": "limit",
        "devId": request_body["devId"],
        "startAt": date_utc,
        "interval": random_interval,
        "maxWh": request_body["maxWh"],
    }

    secret = get_secret()
    write_file_to_s3(response_body, json.loads(secret)["fakeToken"])

    slack_message(request_body["devId"])

    return {
        "statusCode": 200,
        "body": json.dumps(response_body),
        "headers": {"Content-Type": "application/json"},
    }
