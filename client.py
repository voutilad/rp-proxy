#!/usr/bin/env python3
import asyncio
import signal
import sys

import websockets
import common

async def client(uri="ws://127.0.0.1:5000/ws", filters=common.FILTER_DEFAULT):
    async with websockets.connect(uri) as ws:
        # Wire up a signal handler for ctrl-c.
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(
            signal.SIGTERM, loop.create_task, ws.close()
        )

        # Get our filter prompt and reply.
        prompt = await ws.recv()
        if prompt != common.FILTER_PROMPT:
            print("didn't receive our filter prompt!")
            sys.exit(1)
        await ws.send(filters)

        # Start streaming.
        async for message in ws:
            print(message)


if __name__ == "__main__":
    print("starting...")
    asyncio.run(client())
