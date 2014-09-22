# node-busmq

A High performance, highly-available and scalable, message bus and message queueing system for node.js.
Message queues are backed by [Redis](http://redis.io/), a high performance, in-memory key/value store.

### Core Concepts

* High-availability and scalability through the use of multiple redis instances
* Event based message queues
* event based bi-directional channels for peer-to-peer communication (backed by message queues)
* Delivers a message at most once
* Queues are automatically expired after a pre-defined idle time

### High Availability and Scaling

Connecting the bus to multiple redis instances provides high-availability and scaling.

Scaling is achieved by spreading queues and channels between all redis instances. The redis instance
is selected by performing a calculation on the queue/channel name.

High availability is achieved by using standard redis high availability setups, such as
[Redis Sentinal](http://redis.io/topics/sentinel)

## Bus

The bus holds connections to one or more redis instances and is used
to create `queue`s and `channel`s.

Node processes connecting to the same bus have access to and can use all queues and channels.

node-busmq uses the great [node_redis](https://github.com/mranney/node_redis) module to communicate with the redis instances,
so it is highly recommended to also install [hiredis](https://github.com/redis/hiredis-node) to
achieve the best performance.

#### Connecting to a bus

```javascript
var Bus = require('busmq');
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

## Queue

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

#### Using a queue

Producer:

```javascript
bus.on('online', function() {
  var q = bus.queue('foo');
  q.on('attached', function() {
    q.push({hello: 'world'});
    q.push('my name if foo');
    q.detach();
  });
  q.attach();
});
```

Consumer:

```javascript
bus.on('online', function() {
  var q = bus.queue('foo');
  q.on('attached', function() {
    q.consume();
  });
  q.on('message', function(message) {
    if (message === 'my name if foo') {
      q.detach();
    }
  });
  q.attach();
});
```

## Channel

A bi-directional channel for peer-to-peer communication. Under the hood, a channel uses two message queues,
where each peer pushes messages to one queue and consumes messages from the other queue.
It does not matter which peer connects to the channel first.

Each peer in the channel has a role. For all purposes roles are the same, except that the roles determine to which queue messages will be pushed and from which queue they will be consumed. To peers to communicate over the channel, they must have opposite roles.

By default, a channel uses role `local` to consume messages and `remote` to push messages.
Since peers must have opposite roles, if using the default roles, one peer must call `channel#listen` and the other peer must call `channel#connect`.

It is also possible to specify other roles explicity, such as `client` and `server`. This enables specifying the local role and the remote role, and just connecting the channel without calling `listen`. Specifying roles explicitly may add to readability, but not much more than that. 

#### Using a channel (default roles)

Server endpoint:

```javascript
bus.on('online', function() {
  var c = bus.channel('bar'); // use default names for the endpoints
  c.on('connected', function() {
    // connected to the channel
  });
  c.on('remote:connected', function() {
    // the client is connected to the channel
    c.send('hello client!');
  });
  c.on('message', function(message) {
    // received a message from the client
  });
  c.listen(); // reverse the endpoint roles and connect to the channel
});
```

Client endpoint:

```javascript
bus.on('online', function() {
  var c = bus.channel('bar'); // use default names for the endpoints
  c.on('connected', function() {
    // connected to the channel
  });
  c.on('remote:connected', function() {
    // the server is connected to the channel
    c.send('hello server!');
  });
  c.on('message', function(message) {
    // received a message from the server
  });
  c.connect(); // connect to the channel
});
```

#### Using a channel (explicit roles)

Server endpoint:

```javascript
bus.on('online', function() {
  // local role is server, remote role is client
  var c = bus.channel('zoo', 'server', 'client');
  c.on('connected', function() {
    // connected to the channel
  });
  c.on('remote:connected', function() {
    // the client is connected to the channel
    c.send('hello client!');
  });
  c.on('message', function(message) {
    // received a message from the client
  });
  c.connect(); // connect to the channel
});
```

Client endpoint:

```javascript
bus.on('online', function() {
  // notice the reverse order of roles
  // local role is client, remote role is server
  var c = bus.channel('zoo', 'client', 'server');
  c.on('connected', function() {
    // connected to the channel
  });
  c.on('remote:connected', function() {
    // the server is connected to the channel
    c.send('hello server!');
  });
  c.on('message', function(message) {
    // received a message from the server
  });
  c.connect(); // connect to the channel
});
```

## API

Enough with examples. Let's see the API.

### Bus API

##### bus#create()

Create a new bus instance.

##### bus#withLog(log)

Attach a logger to the bus instance. Returns the bus instance.

##### bus#withRedis(redis)

Use the provided `node_redis` client to create connections. Returns the bus instance.

##### bus#connect(redis)

Connect to the specified redis urls. `redis` can be a string or an array of string urls. A valid url has the form `redis://<host_or_ip>[:port]`.

Once connected to all redis instances, the `online` will be emitted.
If the bus gets disconnected from the the redis instances, the `offline` event will be emitted.

##### bus#disconnect()

Disconnect from the redis instances. Once disconnected, the `offline` event will be emitted.

##### bus#isOnline()

Return `true` if the bus is online, `false` if the bus offline.

##### bus#queue(name)

Create a new [Queue](#queue) instance.

* `name` - the name of the queue.

Returns a new Queue instance. Call `queue#attach` before using the queue.

##### bus#channel(name [, local, remote])

Create a new [Channel](#channel) instance.

* `name` - the name of the channel.
* `local` - \[optional\] specifies the local role. default is `local`.
* `remote` - \[optional\] specifies the remote role. default is `remote`.

#### Bus Events

* `online` - emitted when the bus has successfully connected to all of the specified redis instances
* `offline` - emitted when the bus loses connections to the redis instances
* `error` - an error occurs

### Queue API

##### queue#attach([options])

Attach to the queue. If the queue does not already exist it is created.
Once attached, the `attached` event is emitted.

After attaching, it is possible to push and consume messages.

Options:

* `ttl` - duration in seconds for the queue to live without any attachments. default is 30 seconds.

##### queue#detach()

Detach from the queue. The queue will continue to live for as long as it has at least one attachment.
Once a queue has no more attachments, it will continue to exist for the predefined `ttl`, or until it
is attached to again.

##### queue#push(message)

Push a message to the queue. The message can be a JSON object or a string. 
The message will remain in the queue until it is consumed by a consumer.

##### queue#consume([max])

Start consuming messages from the queue.
The `message` event is emitted whenever a message is consumed from the queue.

* `max` if specified, only `max` messages will be consumed from the queue. If not specified,
messages will be continuously consumed as they are pushed into the queue.

##### queue#isConsuming()

Returns `true` if this client is consuming messages, `false` otherwise.

##### queue#stop()

Stop consuming messages from the queue.

##### queue#close()

Closes the queue and destroys all messages. Emits the `closed` event once it is closed.

##### queue#flush()

Empty the queue, removing all messages.

##### queue#exists([callback])

Checks if the queue already exists or not.

* `callback` - receives `true` if the queue exists, `false` otherwise

##### queue#count([callback])

Returns the number if messages in the queue.

* `callback` - receives the number of messages in the queue

##### queue#ttl([callback])

Returns the time in seconds for the queue to live without any attachments.

* `callback` - receives the ttl in seconds

##### queue#metadata(key [, value][, callback])

Get or set arbitrary metadata on the queue.
Will set the metadata `key` to the provided `value`, or get the current value of the key if the `value` parameter is not provided.

* `key` - the metadata key to set or get
* `value` - \[optional\] the value to set on the key.
* `callback` - if setting a metadata value, it is called with no arguments upon success. if retrieving the value,
 it be called with the retrieved value.

##### queue#pushed()

Returns the number of messages pushed by this client to the queue

##### queue#consumed()

Returns the number of messages consumed by this client from the queue

#### Queue Events

* `attaching` - emitted when starting to attach
* `attached` - emitted when attached to the queue. The listener callback receives `true` if the queue already exists
and `false` if it was just created.
* `detaching` - emitted when starting to detach
* `detached` - emitted when detached from the queue. If no other clients are attached to the queue, the queue will remain alive for the `ttl` duration
* `consuming` - emitted when starting or stopping to consume messages from the queue. The listener callback will receive `true`
if starting to consume and `false` if stopping to consume.
* `message` - emitted when a message is consumed from the queue. The listener callback receives the message as a string.
* `error` - emitted when some error occurs. The listener callback receives the error.

### Channel API

##### channel#connect()

Connects to the channel. The `connect` event is emitted once connected to the channel.

##### channel#listen()

Connects to the channel with reverse semantics of the roles. 
The `connect` event is emitted once connected to the channel.

##### channel#send(message)

Send a message to the peer

##### channel#sendTo(endpoint, message)

Send a message to the the specified endpoint. There is no need to connect to the channel with `channel#connect` or `channel#listen`.

##### channel#disconnect()

Disconnect from the channel. The channel remains open and a different peer can connect to it.

##### channel#end()

End the channel. No more messages can be pushed or consumed. This also caused the peer to diconnect from the channel and close the message queues.

##### channel#isBound()

Returns `true` if connected to the channel, `false` if not connected.

#### Channel Events

* `connect` - emitted when connected to the channel
* `remote:connect` - emitted when a remote peer connects to the channel
* `disconnect` - emitted when disconnected from the channel
* `remote:disconnect` - emitted when the remote peer disconnects from the channel
* `message` - emitted when a message is received from the channel. The listener callback receives the message as a string.
* `end` - emitted when the remote peer ends the channel
* `error` - emitted when an error occurs. The listener callback receives the error.

## License

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


