# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2018

import datetime
from tempfile import gettempdir
import json
import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema


def _add_credentials_file(topology, credentials):
    file_name = 'eventstreams.json'
    tmpdirname = gettempdir()
    tmpfile = tmpdirname + '/' + file_name
    with open(tmpfile, "w") as json_file:
        json_file.write(json.dumps(credentials))

    topology.add_file_dependency(tmpfile, 'etc')
    return 'etc/'+file_name


def subscribe(topology, topic, schema, group=None, credentials=None, name=None):
    """Subscribe to messages from Event Streams (Message Hub) for a topic.

    Adds an Event Streams consumer that subscribes to a topic
    and converts each consumed message to a stream tuple.

    Args:
        topology(Topology): Topology that will contain the stream of messages.
        topic(str): Topic to subscribe messages from.
        schema(StreamSchema): Schema for returned stream.
        group(str): Kafka consumer group identifier. When not specified it default to the job name with `topic` appended separated by an underscore.
        credentials(str|dict): Credentials in JSON or name of the application configuration containing the credentials for the Event Streams service. When set to ``None`` the application configuration ``messagehub`` is used.
        name(str): Consumer name in the Streams context, defaults to a generated name.

    Returns:
         Stream: Stream containing messages.
    """
    if schema is CommonSchema.Json:
        msg_attr_name='jsonString'
    elif schema is CommonSchema.String:
        msg_attr_name='string'
    else:
        raise TypeError(schema)

    if group is None:
        group = streamsx.spl.op.Expression.expression('getJobName() + "_" + "' + str(topic) + '"')

    if name is None:
        name = topic

    appConfigName=credentials
    # check if it's the credentials for the service
    if isinstance(credentials, dict):
         appConfigName = None

    _op = _MessageHubConsumer(topology, schema=schema, outputMessageAttributeName=msg_attr_name, appConfigName=appConfigName, topic=topic, groupId=group, name=name)
    if isinstance(credentials, dict):
         _op.params['messageHubCredentialsFile'] = _add_credentials_file(topology, credentials)
    return _op.stream


def publish(stream, topic, credentials=None, name=None):
    """Publish Event Streams (Message Hub) messages to a topic.

    Adds an Event Streams producer where each tuple on `stream` is
    published as a stream message.

    Args:
        stream(Stream): Stream of tuples to published as messages.
        topic(str): Topic to publish messages to.
        credentials(str|dict): Credentials in JSON or name of the application configuration containing the credentials for the Event Streams service. When set to ``None`` the application configuration ``messagehub`` is used.
        name(str): Producer name in the Streams context, defaults to a generated name.

    Returns:
        streamsx.topology.topology.Sink: Stream termination.
    """
    if stream.oport.schema == CommonSchema.Json:
        msg_attr = 'jsonString'
    elif stream.oport.schema == CommonSchema.String:
        msg_attr = 'string'
    else:
        raise TypeError(schema)

    appConfigName=credentials
    # check if it's the credentials for the service
    if isinstance(credentials, dict):
         appConfigName = None

    _op = _MessageHubProducer(stream, appConfigName=appConfigName, topic=topic)
    _op.params['messageAttribute'] = _op.attribute(stream, msg_attr)
    if isinstance(credentials, dict):
         _op.params['messageHubCredentialsFile'] = _add_credentials_file(stream.topology, credentials)

    return streamsx.topology.topology.Sink(_op)


class _MessageHubConsumer(streamsx.spl.op.Source):
    def __init__(self, topology, schema, vmArg=None, appConfigName=None, clientId=None, messageHubCredentialsFile=None, outputKeyAttributeName=None, outputMessageAttributeName=None, outputTimestampAttributeName=None, outputOffsetAttributeName=None, outputPartitionAttributeName=None, outputTopicAttributeName=None, partition=None, propertiesFile=None, startPosition=None, startTime=None, topic=None, triggerCount=None, userLib=None, groupId=None, name=None):
        kind="com.ibm.streamsx.messagehub::MessageHubConsumer"
        inputs=None
        schemas=schema
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if clientId is not None:
            params['clientId'] = clientId
        if messageHubCredentialsFile is not None:
            params['messageHubCredentialsFile'] = messageHubCredentialsFile
        if outputKeyAttributeName is not None:
            params['outputKeyAttributeName'] = outputKeyAttributeName
        if outputMessageAttributeName is not None:
            params['outputMessageAttributeName'] = outputMessageAttributeName
        if outputTimestampAttributeName is not None:
            params['outputTimestampAttributeName'] = outputTimestampAttributeName
        if outputOffsetAttributeName is not None:
            params['outputOffsetAttributeName'] = outputOffsetAttributeName
        if outputPartitionAttributeName is not None:
            params['outputPartitionAttributeName'] = outputPartitionAttributeName
        if outputTopicAttributeName is not None:
            params['outputTopicAttributeName'] = outputTopicAttributeName
        if partition is not None:
            params['partition'] = partition
        if propertiesFile is not None:
            params['propertiesFile'] = propertiesFile
        if startPosition is not None:
            params['startPosition'] = startPosition
        if startTime is not None:
            params['startTime'] = startTime
        if topic is not None:
            params['topic'] = topic
        if triggerCount is not None:
            params['triggerCount'] = triggerCount
        if userLib is not None:
            params['userLib'] = userLib
        if groupId is not None:
            params['groupId'] = groupId
        super(_MessageHubConsumer, self).__init__(topology,kind,schemas,params,name)



class _MessageHubProducer(streamsx.spl.op.Sink):
    def __init__(self, stream, vmArg=None, appConfigName=None, keyAttribute=None, messageAttribute=None, messageHubCredentialsFile=None, partitionAttribute=None, propertiesFile=None, timestampAttribute=None, topicAttribute=None, topic=None, userLib=None, name=None):
        topology = stream.topology
        kind="com.ibm.streamsx.messagehub::MessageHubProducer"
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if keyAttribute is not None:
            params['keyAttribute'] = keyAttribute
        if messageAttribute is not None:
            params['messageAttribute'] = messageAttribute
        if messageHubCredentialsFile is not None:
            params['messageHubCredentialsFile'] = messageHubCredentialsFile
        if partitionAttribute is not None:
            params['partitionAttribute'] = partitionAttribute
        if propertiesFile is not None:
            params['propertiesFile'] = propertiesFile
        if timestampAttribute is not None:
            params['timestampAttribute'] = timestampAttribute
        if topicAttribute is not None:
            params['topicAttribute'] = topicAttribute
        if topic is not None:
            params['topic'] = topic
        if userLib is not None:
            params['userLib'] = userLib
        super(_MessageHubProducer, self).__init__(kind,stream,params,name)
