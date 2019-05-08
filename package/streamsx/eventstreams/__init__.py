# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

"""
Overview
++++++++

`IBM® Event Streams <https://www.ibm.com/cloud/event-streams>`_ is a fully managed, cloud-based messaging service. It is built on Apache Kafka and is available through IBM Cloud® as a Service.

This module allows a Streams application to :py:func:`subscribe <subscribe>` a
message queue as a stream and :py:func:`publish <publish>` messages on a queue from a stream
of tuples.

Credentials
+++++++++++

Event Streams credentials are defined using a Streams application configuration or setting the Event Streams service credentials JSON directly to the ``credentials`` parameter of the functions.

By default an application configuration named `messagehub` is used,
a different configuration can be specified using the ``credentials``
parameter to :py:func:`subscribe` or :py:func:`publish`.

The application configuration must contain the property ``messagehub.creds`` with a value of the raw Event Streams service credentials JSON.

Messages
++++++++

The schema of the stream defines how messages are handled.

* ``CommonSchema.String`` - Each message is a UTF-8 encoded string.
* ``CommonSchema.Json`` - Each message is a UTF-8 encoded serialized JSON object.

No other formats are supported.

Sample
++++++

A simple hello world example of a Streams application publishing to
a topic and the same application consuming the same topic::

    from streamsx.topology.topology import Topology
    from streamsx.topology.schema import CommonSchema
    from streamsx.topology.context import submit
    import streamsx.eventstreams as eventstreams
    import time

    def delay (v):
    time.sleep (5.0)
    return True

    topo = Topology ('EventStreamsHelloWorld')

    to_evstr = topo.source (['Hello', 'World!'])
    to_evstr = to_evstr.as_string()
    # delay tuple by tuple
    to_evstr = to_evstr.filter (delay)

    # Publish a stream to Event Streams using HELLO topic
    eventstreams.publish (to_evstr, topic='HELLO')

    # Subscribe to same topic as a stream
    from_evstr = eventstreams.subscribe (topo, schema=CommonSchema.String, topic='HELLO')

    # You'll find the Hello World! in stdout log file:
    from_evstr.print()

    submit ('STREAMING_ANALYTICS_SERVICE', topo)

"""

__version__='1.1.0'

__all__ = ['subscribe', 'publish', 'configure_connection']
from streamsx.eventstreams._eventstreams import subscribe, publish, configure_connection
