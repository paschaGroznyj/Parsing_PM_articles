from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack

from aiobotocore.client import AioBaseClient
from aiobotocore.session import AioSession
from botocore.client import Config
from dotenv import load_dotenv
import os
import asyncio

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

if __name__ == "__main__":
    asyncio.run(put_object())