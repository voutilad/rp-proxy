#!/usr/bin/env python3
from os import environ

from quart import Quart, websocket
from redpanda import Redpanda
import common

from logging.config import dictConfig
import asyncio


RP_USER=environ.get("REDPANDA_USER", None)
RP_PASS=environ.get("REDPANDA_PASS", None)
RP_URL=environ.get("REDPANDA_BROKERS", None)
RP_MECHANISM=environ.get("REDPANDA_MECHANISM", None)
RP_TLS=True if environ.get("REDPANDA_TLS", False) else False

REDPANDA = Redpanda(
    RP_URL,
    username=RP_USER,
    password=RP_PASS,
    mechanism=RP_MECHANISM,
    tls=RP_TLS,
)
TASK = None

app = Quart(__name__)

@app.before_serving
async def startup():
    app.logger.info(f"connecting to Redpanda @ {RP_URL}")
    await REDPANDA.connect()
    TASK = asyncio.ensure_future(REDPANDA.poll())


@app.after_serving
async def shutdown():
    app.logger.info("disconnecting from Redpanda")
    await REDPANDA.disconnect()
    if TASK:
        TASK.join()


@app.websocket("/ws")
async def handle_connection():
    q = None
    client_ip = websocket.remote_addr

    try:
        app.logger.info(f"connection from {client_ip}")
        await websocket.send(common.PROMPT)

        subscription = (await websocket.receive()).decode("utf8").strip()
        if common.DELIM in subscription:
            parts = subscription.split(common.DELIM)
            if len(parts) > 2:
                app.logger.warn("invalid subscription requested")
                await websocket.send("bad subscription")
                return
            (topic, key_filter) = parts
        else:
            topic = subscription
            key_filter = None

        app.logger.info(
            f"{client_ip} subscribing to {topic}{common.DELIM}{key_filter}"
        )
        q = await REDPANDA.subscribe(topic, key_filter=key_filter)

        # Poll for values from the queue and send to the listener.
        # TODO: could cut out the middle man and have the Redpanda poll() routine
        #       directly write the data to this connection.
        while True:
            value = await q.get()
            await websocket.send(str(value))
    except asyncio.CancelledError:
        # Handle disconnects cleanly.
        app.logger.info(f"{client_ip} disconnected")
        if q:
            REDPANDA.unsubscribe(topic, key_filter, q)


if __name__ == "__main__":
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

    ### Fire up the engines.
    app.run()
