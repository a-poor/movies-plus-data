import os
from io import BytesIO
import json
import boto3
import requests


def download_image(url: str) -> bytes:
    return requests.get(url).content

def upload_image(bucket_name: str, file_name: str, data: bytes):
    s3 = boto3.client("s3")
    f = BytesIO(data)
    s3.upload_fileobj(f,bucket_name,file_name)

def make_s3_path(bucket_name: str, file_name: str) -> str:
    return f"s3://{bucket_name}/{file_name}"


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        API Gateway Lambda Proxy Input Format

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    API Gateway Lambda Proxy Output Format: dict

        Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
    """

    params = json.loads(event["body"])
    req_params = {"img-url","bucket","filename"}

    # return {
    #     "statusCode": 200,
    #     "body": json.dumps(event)
    # }
    
    # Check for correct params
    for p in req_params:
        if p not in params or not isinstance(params[p],str):
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "success": False,
                    "message": f"Bad Parameters | Must specify str params: {req_params}"
                })
            }

    # Extract params
    img_url = params["img-url"]
    bucket_name = params["bucket"]
    file_name = params["filename"]
        
    # Download the image
    try:
        img_data = download_image(img_url)
    except Exception as e:
        return {
            "statusCode": 200,
                "body": json.dumps({
                    "success": False,
                    "message": f"Error downloading image: {type(e)}{e}"
            })
        }

    # Upload the image to S3
    try:
        upload_image(bucket_name, file_name, img_data)
    except Exception as e:
        return {
            "statusCode": 200,
                "body": json.dumps({
                    "success": False,
                    "message": f"Error downloading image: {type(e)}{e}"
            })
        }

    # Return the image's new S3 location
    return {
        "statusCode": 200,
        "body": json.dumps({
            "success": True,
            "img-path": make_s3_path(bucket_name, file_name)
        }),
    }
