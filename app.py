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
    app.logger.info(f"Connecting to Redpanda @ {RP_URL}")
    await REDPANDA.connect()
    TASK = asyncio.ensure_future(REDPANDA.poll())

@app.after_serving
async def shutdown():
    app.logger.info("Disconnecting from Redpanda")
    await REDPANDA.disconnect()
    if TASK:
        TASK.join()

@app.route("/")
async def index():
    return f"""
    <html><body>
    <p>
    	Hey, I'm connected to: {RP_URL}
    </p>
    <p>
    	To subscribe, hit me up here: <a href="http://localhost:5000/subscribe?topic=test">http://localhost:5000/subscribe</a>
    </p>
    </body></html>
    """

@app.websocket("/ws")
async def handle_connection():
    q = None
    try:
        app.logger.info("handling client")
        await websocket.send(common.FILTER_PROMPT)
        app.logger.info("waiting on client to tell me its desired filters")
        topic = (await websocket.receive()).strip()
        app.logger.info(f"subscribing to topic ${topic}")
        q = await REDPANDA.subscribe(topic)
        while True:
            value = await q.get()
            await websocket.send(str(value))
    except asyncio.CancelledError:
        # handle disconnect
        app.logger.info("disconnecting")
        if q:
            REDPANDA.unsubscribe(topic, q)


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
