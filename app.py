#!/usr/bin/env python3
import argparse
from os import environ

from quart import Quart, websocket
from redpanda import Redpanda
import common

from logging.config import dictConfig
import asyncio


## Globals
REDPANDA = None
TASK = None
APP = Quart(__name__)


@APP.before_serving
async def startup():
    APP.logger.info(f"connecting to Redpanda @ {REDPANDA.brokers}")
    await REDPANDA.connect()
    TASK = asyncio.ensure_future(REDPANDA.poll())


@APP.after_serving
async def shutdown():
    APP.logger.info("disconnecting from Redpanda")
    await REDPANDA.disconnect()
    if TASK:
        TASK.join()


@APP.websocket("/ws")
async def handle_connection():
    q = None
    client_ip = websocket.remote_addr

    try:
        APP.logger.info(f"connection from {client_ip}")
        await websocket.send(common.PROMPT)

        subscription = (await websocket.receive()).decode("utf8").strip()
        if common.DELIM in subscription:
            parts = subscription.split(common.DELIM)
            if len(parts) > 2:
                APP.logger.warn("invalid subscription requested")
                await websocket.send("bad subscription")
                return
            (topic, key_filter) = parts
        else:
            topic = subscription
            key_filter = None

        APP.logger.info(
            f"{client_ip} subscribing to {topic}{common.DELIM}{key_filter}"
        )
        q = await REDPANDA.subscribe(topic, key_filter=key_filter)

        # Poll for values from the queue and send to the listener.
        # TODO: could cut out the middle man and have the Redpanda poll() routine
        #       directly write the data to this connection.
        while True:
            (offset, key, value) = await q.get()
            # TODO: relies on websocket framing for now
            await websocket.send(key)
            await websocket.send(value)
    except asyncio.CancelledError:
        # Handle disconnects cleanly.
        APP.logger.info(f"{client_ip} disconnected")
        if q:
            REDPANDA.unsubscribe(topic, key_filter, q)


if __name__ == "__main__":
    ### Parse our arguments.
    parser = argparse.ArgumentParser(description="Simple Redpanda WS proxy")
    parser.add_argument("-b", "--brokers", default="127.0.0.1:9092")
    parser.add_argument("-u", "--user", default=None)
    parser.add_argument("-p", "--password", default=None)
    parser.add_argument("-m", "--sasl-mechanism", default=None)
    parser.add_argument("-t", "--enable-tls", default=False,
                        action=argparse.BooleanOptionalAction)
    args = parser.parse_args()
    REDPANDA = Redpanda(
        args.brokers,
        username=args.user,
        password=args.password,
        mechanism=args.sasl_mechanism,
        tls=args.enable_tls,
    )

    ###
    # Use Hypercorn's native formatting so things look pretty in dev mode.
    ###
    format = "%(asctime)s [%(process)d] [%(levelname)s] %(message)s"
    datefmt = "[%Y-%m-%d %H:%M:%S %z]"
    dictConfig({
        "version": 1,
        "formatters": { "simple": { "format": format, "datefmt": datefmt } },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "DEBUG",
                "stream": "ext://sys.stdout",
                "formatter": "simple",
            },
        },
        "loggers": {
            "app": { "level": "DEBUG", "handlers": ["console"]},
            "redpanda": { "level": "INFO", "handlers": ["console"] },
        },
    })

    ### Fire up the engines. Make rocket go now.
    APP.run()
