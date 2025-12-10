from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack

from aiobotocore.client import AioBaseClient
from aiobotocore.session import AioSession
from botocore.client import Config
from dotenv import load_dotenv
import os
import asyncio
import json

load_dotenv()

params = {
    "service_name": "s3",
    "aws_access_key_id": os.getenv("OBS_ACCESS_KEY"),
    "aws_secret_access_key": os.getenv("OBS_SECRET_KEY"),
    "region_name": os.getenv("OBS_REGION"),
    "endpoint_url": os.getenv("OBS_ENDPOINT"),
    "config": Config(s3={"addressing_style": "virtual"})
}

print(params)

async def create_async_client(session: AioSession, exit_stack: AsyncExitStack):
    context_manager = session.create_client(**params)
    client = await exit_stack.enter_async_context(context_manager)
    return client


async def get_async_client() -> AsyncGenerator[AioBaseClient, None]:
    session = AioSession()
    async with AsyncExitStack() as exit_stack:
        client = await create_async_client(session, exit_stack)
        yield client


# ---------------------------
#      Тестовый сценарий
# ---------------------------
async def put_object():
    session = AioSession()
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(
            session.create_client(**params)
        )

        bucket = os.getenv("OBS_BUCKET") # cds-bucket
        key = "test_upload.txt"

        body = b"hello world"

        print("Uploading...")

        await client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType="text/plain"
        )

        print("Downloading...")
        response = await client.get_object(Bucket=bucket, Key=key)

        data = await response["Body"].read()
        print("Downloaded:", data)

async def put_json():
    session = AioSession()
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(
            session.create_client(**params)
        )

        bucket = os.getenv("OBS_BUCKET") # cds-bucket
        key = "articles/parsed_data.json"

        with open("articles.json", "r") as file:
            data = json.load(file)


        json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")

        await client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_bytes,
            ContentType="application/json"
        )

async def upload_all_md():
    session = AioSession()

    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(
            session.create_client(**params)
        )

        bucket = os.getenv("OBS_BUCKET")

        for filename in os.listdir("result_docs"):
            if filename.endswith(".md"):
                local_path = os.path.join("result_docs", filename)

                key = f"articles/md/{filename}"  # путь в S3

                with open(local_path, "rb") as f:
                    await client.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=f,
                        ContentType="text/markdown"
                    )

                print(f"Uploaded {filename} → {key}")

async def delete_dir(client, bucket: str, prefix: str):
    """
    Удаляет все объекты по заданному префиксу (папку) в OBS/S3.
    """

    continuation_token = None

    while True:
        list_params = {
            "Bucket": bucket,
            "Prefix": prefix
        }

        if continuation_token:
            list_params["ContinuationToken"] = continuation_token

        resp = await client.list_objects_v2(**list_params)

        # Нет объектов
        if "Contents" not in resp:
            break

        # Удаляем пачками
        delete_payload = {
            "Objects": [{"Key": obj["Key"]} for obj in resp["Contents"]],
            "Quiet": True
        }

        await client.delete_objects(
            Bucket=bucket,
            Delete=delete_payload
        )

        # Проверяем, есть ли ещё страницы
        if resp.get("IsTruncated"):
            continuation_token = resp.get("NextContinuationToken")
        else:
            break

    print(f"Удалено всё по пути: {prefix}")

async def del_md_dir():
    session = AioSession()
    async with AsyncExitStack() as stack:
        client = await stack.enter_async_context(
            session.create_client(**params)
        )

        bucket = os.getenv("OBS_BUCKET")
        await delete_dir(
            client,
            bucket=bucket,
            prefix="articles/md/"
        )

if __name__ == "__main__":

    asyncio.run(put_json())