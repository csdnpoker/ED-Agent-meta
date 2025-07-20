# receiver.py
import asyncio
from nats.aio.client import Client as NATS

async def main():
    nc = NATS()
    #置为meta的公网IP
    await nc.connect("nats://106.120.188.128:4222")

    async def message_handler(msg):
        print(f"Received a message on '{msg.subject}': {msg.data.decode()}")

    #注释掉自己
    #await nc.subscribe("meta", cb=message_handler)
    await nc.subscribe("sub1", cb=message_handler)
    await nc.subscribe("sub2", cb=message_handler)
    await nc.subscribe("sub3", cb=message_handler)
    await nc.subscribe("sub4", cb=message_handler)
    await nc.subscribe("sub5", cb=message_handler)
    await nc.subscribe("sub6", cb=message_handler)
    await nc.subscribe("sub7", cb=message_handler)
    await nc.subscribe("sub8", cb=message_handler)
    await nc.subscribe("sub9", cb=message_handler)
    await nc.subscribe("sub10", cb=message_handler)
    await nc.subscribe("sub11", cb=message_handler)
    await nc.subscribe("sub12", cb=message_handler)
    await nc.subscribe("sub13", cb=message_handler)


    print("Listening on subject 'updates'...")
    while True:
        await asyncio.sleep(1)

asyncio.run(main())
