#!/usr/bin/env python3
import argparse
import asyncio
import signal
import sys

import websockets
import websockets.exceptions as wse
import common

async def client(uri="ws://127.0.0.1:5000/ws", topic=common.DEFAULT_TOPIC,
                 key_filter=common.MATCH_ALL):
    """
    Starts a simple WebSocket client to our Redpanda WS Proxy service.
    """
    async with websockets.connect(uri) as ws:
        # Get our filter prompt and reply.
        prompt = await ws.recv()
        if prompt != common.PROMPT:
            print("didn't receive our filter prompt!")
            sys.exit(1)
        print(f"connected to {uri}", file=sys.stderr)
        await ws.send(f"{topic}{common.DELIM}{key_filter}".encode())

        # Start streaming.
        print(f"listening for messages from {topic}{common.DELIM}{key_filter}",
              file=sys.stderr)
        try:
            while True:
                key = await ws.recv()
                value = await ws.recv()
                print((key, value))
        except wse.ConnectionClosedError:
            print("server disconnected us", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple WebSocket client")
    parser.add_argument("-u", "--uri", default="ws://127.0.0.1:5000/ws")
    parser.add_argument("-t", "--topic", default=common.DEFAULT_TOPIC)
    parser.add_argument("-k", "--key", default=common.MATCH_ALL)
    args = parser.parse_args()

    try:
        asyncio.run(client(args.uri, topic=args.topic, key_filter=args.key))
    except KeyboardInterrupt:
        pass
