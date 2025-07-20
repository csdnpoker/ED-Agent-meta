# sender.py
import asyncio
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    #置为meta的公网IP
    await nc.connect("nats://106.120.188.128:4222")
    #频道名置为自己的服务器名
    await nc.publish("meta", b'Hello NATS!')
    print("Message published to 'updates'")

    await nc.drain()

asyncio.run(main())
