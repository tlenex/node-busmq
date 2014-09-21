# BusMQ

A High performance, highly-available and scalable, message bus and message queueing system for node.js.
Message queues are backed by [Redis](http://redis.io/), a high performance, in-memory key/value store.

## Core Concepts

* High-availability and scalability through the use of multiple redis instances
* Event based message queues and channels. publish/subscribe functionality _is not_ provided
* Delivers a message at most once
* Provides a bi-directional channel for peer-to-peer communication (backed by message queues)
* Queues are automatically expired after a pre-defined idle time

## High Availability and Scaling

Connecting the bus to multiple redis instances provides high-availability and scaling.

Scaling is achieved by spreading queues and channels between all redis instances. The redis instance
is selected by performing a calculation on the queue/channel name.

High availability is achieved by using standard redis high availability setups, such as
[Redis Sentinal](http://redis.io/topics/sentinel)

### Bus

The bus holds connections to one or more redis instances and is used
to create ``queue``s and ``channel``s.

Node processes connecting to the same bus have access to and can use all queues and channels.

BusMQ uses the great [node_redis](https://github.com/mranney/node_redis) module to communicate with the redis instances,
so it is highly recommended to also install [hiredis](https://github.com/redis/hiredis-node) to
achieve the best performance.

#### Connecting to a bus

Nothing better than an example:

```javascript
var Bus = require('node-busmq');
var bus = Bus.create();
bus.on('error', function(err) {
  // an error has occurred
});
bus.on('online', function() {
  // the bus is online - we can create queues
});
bus.on('offline', function() {
  // the bus is offline - redis is down...
});

// connect to a single redis instance
bus.connect('redis://192.168.0.1:6359');

// or, connect to multiple redis instances
bus.connect(['redis://192.168.0.1:6359', 'redis://192.168.0.2:6359']);
```

#### Bus API

##### ``bus#create()``

Create a new BusMQ instance.

##### ``bus#withLog(log)``

Attach a logger to the bus instance. Return the bus instance.

##### ``bus#withRedis(redis)``

Use the provided ``node_redis`` client to create connections. Return the bus instance.

##### ``bus#connect(redis)``

Connect to the specified redis urls. ``redis`` can be a string or an array of string urls. A valid url has the form ``redis://<host_or_ip>[:port]``.

Once connected to all redis instances, the ``online`` will be emitted. \
If the bus gets disconnected from the the redis instances, the ``offline`` event will be emitted.

##### ``bus#disconnect()``

Disconnect from the redis instances. Once disconnected, the ``offline`` event will be emitted.

##### ``bus#isOnline()``

Return ``true`` if the bus is online, ``false`` if the bus offline.

##### ``bus#queue(name)``

Create a new [Queue](#Queue) instance.

* ``name`` - the name of the queue.

Returns a new Queue instance. Call ``queue#attach`` before using the queue.

##### ``bus#channel(name, [local, remote])``

Create a new [Channel](#Channel) instance.

* ``name`` - the name of the channel.
* ``local`` - \[optional\]. Specifies the local endpoint name of the channel. default is ``local``.
* ``remote`` - \[optional\]. Specifies the remote endpoint name of the channel. default is ``remote``.

#### Bus Events

The bus emits the following events:

* ``online`` - emitted when the bus has successfully connected to all of the specified redis instances
* ``offline`` - emitted when the bus loses connections to the redis instances
* ``error`` - an error occurs

### Queue

A queue of messages.

Messages are consumed in they order that they are pushed into the queue.
Once a message is consumed, it will never be consumed again.

Any number of clients can produce messages to a queue, and any number of consumers
can consume messages from a queue. A message is consumed by one consumer at most.

#### Attach and detach

To push and consume messages, first attach to the queue.
Once attached, it is possible to push messages and start consuming messages from the queue.
The queue will remain in existence for as long as it has at least one attachment.

To stop using a queue, detach from it. Once a queue has no more attachments, it will automatically expire
after a predefined ttl (losing any messages in the queue).

```javascript
var q = bus.queue('myqueue');
q.attach();

// ... do some stuff ...

q.detach();
```

#### Queue API

##### ``queue#attach([options])``

Attach to the queue. If the queue does not already exist it is created.
Once attached, the ``attached`` event is emitted.

After attaching, it is possible to push and consume messages.

Options:

* ``ttl`` - duration in seconds for the queue to live without any attachments. default is 30 seconds.

##### ``queue#detach()``

Detach from the queue. The queue will continue to live for as long as it has at least one attachment.
Once a queue has no more attachments, it will continue to exist for the predefined ``ttl``, or until it
is attached to again.

#### Queue Events



### Channel

A bi-directional channel for peer-to-peer communication. Under the hood, a channel uses two message queues,
where each peer pushes messages to one queue and consumes messages from the other queue.
It does not matter which peer connects to the channel first.

#### Channel API

#### Channel Events

# License

The MIT License (MIT)

Copyright (c) 2014 Capriza Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.


