# BusMQ

A High performance, highly available and scalable, production grade, message bus and message queueing system for node.js.
Message queues are backed by [Redis](http://redis.io/), a high performance, in-memory key/value store.

## Core Concepts

* Provides high-availability through the use of multiple node.js instances
* Provides scalability through the use of multiple redis instances
* Event based message queues and channels. publish/subscribe functionality _is not_ provided
* Deliver a message at most once
* Provides a bi-directional, virtual channel backed by message queues
* Does not leak unused message queues - queues are automatically expired after a pre-defined idle time

### Queues

A queue of messages. Messages will are consumed in they order that they are pushed into the queue.
Once a message is consumed, it will never be consumed again.

Any number of clients can produce messages to a queue, and any number of consumers
can consume messages from a queue. A message can only be consumed by one consumer.

### Channels

A bi-directional channel for communicating between two peers.
It does not matter which peer connects to the channel first.

## Usage



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


