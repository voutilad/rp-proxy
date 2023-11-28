# "Selective Fan-out" Example with redpanda
> or: _don't call it pub/sub_

This is a sample/prototype of what a solution might look like to the
problem of having significantly more data consumers than a Kafka-based
system would elegantly support through simply partioning a
topic.

Specifically, these data consumers are very _selective_ about what
they want to consume because they can't each have their own
topic-partition. In other words, they may care about a fraction of a
partition of a stream. How might we design something that:

  - supports an order of magnitude (or more) data consumers than Kafka
    Consumers, allowing constraining topic/partition counts to
    appropriate levels based on the cluster resources,
  - lets data consumers come and go and receive the data they want,
  - and can be optimized for latency?

In this specific scenario, we **don't care** about:

  - exactly once semantics (EOS),
  - seeing every applicable message; it's ok to miss some data,
  - and minimizing network utilization between Broker and Consumer.

> Note: in this doc when I mean a Kafka Consumer, I'll use a big
> 'C'. For an arbitrary piece of code that has interest in a
> particular message on a topic, I refer to it as a data consumer
> using a small 'c'.

## Isn't this Pub/Sub?

Sort of? What this _isn't doing_ is pure pub/sub semantics. Using a
Kafka topic allows for retention, which means data consumers that
_don't care about this pub/sub paradigm_ can still leverage Kafka
stream concepts with the same Redpanda cluster.

## Approach

1. Skip using consumer groups and assign partitions manually to a
   Consumer, allowing us to have more than one Consumer to partition.

2. Filter at the Consumer and distribute messages to data consumers.

3. Stay at the latest offset/head of the stream. Don't care about
   messages prior to a data consumer "susbcribing".


## Running

Spin up a Redpanda instance somewhere. In my case, I'm using Redpanda
Serverless (in private beta). Create a Kafka API user (e.g. `python`)
that has permissions to produce to a topic.

Set up your virtual environment and install dependencies:

```
$ python3 -m venv venv
$ . venv/bin/activate
(venv) $ pip install -r requirements.txt
```

Start up a server:

```
$ ./app.py \
    --brokers tktktk.any.us-east-1.mpx.prd.cloud.redpanda.com:30092 \
    --user python \
    --password purple-monkey-dishwasher \
    --sasl-mechanism SCRAM-SHA-256 \
    --enable-tls
 * Serving Quart app 'app'
 * Debug mode: False
 * Please use an ASGI server (e.g. Hypercorn) directly in production
 * Running on http://127.0.0.1:5000 (CTRL + C to quit)
[2023-11-28 10:54:24 -0500] [99652] [INFO] connecting to Redpanda @ tktktk.any.us-east-1.mpx.prd.cloud.redpanda.com:30092
[2023-11-28 10:54:25 -0500] [99652] [INFO] discovered topics: {'hello-world', 'incoming'}
[2023-11-28 10:54:25 -0500] [99652] [INFO] Running on http://127.0.0.1:5000 (CTRL + C to quit)
```

> Obviously, that's not a valid broker seed or username/password ;)

Start up one or many clients:

```
$ ./client.py \
    --uri "ws://127.0.0.1:5000/ws" \
    --topic incoming \
    --key dave
connected to ws://127.0.0.1:5000/ws
listening for messages from incoming/dave
```

Produce some data to your topic, providing a key that matches or does
not match your intended key filters.

If you want to just receive _all_ messages from a topic, use the
special "match all" key of `*`:

```
./client.py \
    --uri "ws://127.0.0.1:5000/ws" \
    --topic incoming \
    --key "*"
connected to ws://127.0.0.1:5000/ws
listening for messages from incoming/*
```

In either case, you should see data arrive on the client side showing
the format of `(<offset>, <key>, <value>)` on the console:

```
connected to ws://127.0.0.1:5000/ws
listening for messages from incoming/dave
(15, b'dave', b'Hey, Dave, did you get this?')
```

## Left to the Reader

1. This prototype uses websockets, but doesn't handle TLS on the
   consumer side. Easy to add with `quart` but this was done quickly.

2. The data types and algorithms for handling subscriptions is most
   likely suboptimal. A 2-layer map structure is probably not great!

3. Key hashing isn't being used on the registered patterns, so we're
   not intelligently consuming from _only_ the partitions that might
   match a subscription. Lots of data read only to be dropped!

4. Both keys and values are treated as opaque byte arrays and no
   Schema Registry integration is used.

5. No actual tuning was done for latency. There are some constants
   that should be tuned as needed, but don't forget the Producer side,
   too.

6. The double-buffering in the form of putting messages into
   individual queues for relay to the data consumers is an extra step
   and data copy that's definitely going to add latency at scale. This
   could definitely be reworked.
