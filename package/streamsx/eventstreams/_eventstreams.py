# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2018

from tempfile import gettempdir
import json
import streamsx.spl.op
import streamsx.spl.types
from streamsx.topology.schema import CommonSchema
from streamsx.eventstreams.schema import Schema
from streamsx.toolkits import download_toolkit

_TOOLKIT_NAME = 'com.ibm.streamsx.messagehub'


def _add_toolkit_dependency(topo):
    # IMPORTANT: Dependency of this python wrapper to a specific toolkit version
    # This is important when toolkit is not set with streamsx.spl.toolkit.add_toolkit (selecting toolkit from remote build service)
    # messagehub toolkit >= 1.7.0 support the 'credentials' parameter were we can pass JSON directly to the operators
    streamsx.spl.toolkit.add_toolkit_dependency(topo, _TOOLKIT_NAME, '[1.7.0,99.0.0)')


def _add_credentials_file(topology, credentials):
    """
    Adds a file dependency to the topology.
    The file contains the credentials as JSON.
    The filename in the bundle is ``etc/eventstreams.json``.
    """
    if credentials is None:
        raise TypeError(credentials)
    file_name = 'eventstreams.json'
    tmpdirname = gettempdir()
    tmpfile = tmpdirname + '/' + file_name
    with open(tmpfile, "w") as json_file:
        json_file.write(json.dumps(credentials))

    topology.add_file_dependency(tmpfile, 'etc')
    fName = 'etc/'+file_name
    print("Adding file dependency " + fName + " to the topology " + topology.name)
    return fName


def download_toolkit(url=None, target_dir=None):
    r"""Downloads the latest streamsx.messagehub toolkit from GitHub.

    Example for updating the toolkit for your topology with the latest toolkit from GitHub::

        import streamsx.eventstreams as es
        # download the toolkit from GitHub
        toolkit_location = es.download_toolkit()
        # add the toolkit to topology
        streamsx.spl.toolkit.add_toolkit(topology, toolkit_location)

    Example for updating the topology with a specific version of the streamsx.messagehub toolkit using an URL::

        import streamsx.eventstreams as es
        url202 = 'https://github.com/IBMStreams/streamsx.messagehub/releases/download/v2.0.2/com.ibm.streamsx.messagehub-2.0.2.tgz'
        toolkit_location = es.download_toolkit(url=url202)
        streamsx.spl.toolkit.add_toolkit(topology, toolkit_location)

    Args:
        url(str): Link to toolkit archive (\*.tgz) to be downloaded. Use this parameter to 
            download a specific version of the toolkit.
        target_dir(str): the directory where the toolkit is unpacked to. If a relative path is given,
            the path is appended to the system temporary directory, for example to /tmp on Unix/Linux systems.
            If target_dir is ``None`` a location relative to the system temporary directory is chosen.

    Returns:
        str: the location of the downloaded streamsx.messagehub toolkit

    .. note:: This function requires an outgoing Internet connection
    .. versionadded:: 1.3
    """
    _toolkit_location = streamsx.toolkits.download_toolkit (toolkit_name=_TOOLKIT_NAME, url=url, target_dir=target_dir)
    return _toolkit_location


def configure_connection(instance, name='messagehub', credentials=None):
    """Configures IBM Streams for a certain connection.


    Creates an application configuration object containing the required properties with connection information.


    Example for creating a configuration for a Streams instance with connection details::


        from streamsx.rest import Instance
        import streamsx.topology.context
        from icpd_core import icpd_util

        cfg = icpd_util.get_service_instance_details(name='your-streams-instance')
        cfg[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
        instance = Instance.of_service(cfg)
        app_cfg = configure_connection(instance, credentials='my_crdentials_json')


    Args:
        instance(streamsx.rest_primitives.Instance): IBM Streams instance object.
        name(str): Name of the application configuration, default name is 'messagehub'.
        credentials(str|dict): The service credentials for Eventstreams.
    Returns:
        Name of the application configuration.

    .. warning:: The function can be used only in IBM Cloud Pak for Data.
    .. versionadded:: 1.1
    """

    description = 'Eventstreams credentials'
    properties = {}
    if credentials is None:
        raise TypeError(credentials)

    if isinstance(credentials, dict):
        properties['messagehub.creds'] = json.dumps(credentials)
    else:
        properties['messagehub.creds'] = credentials

    # check if application configuration exists
    app_config = instance.get_application_configurations(name=name)
    if app_config:
        print('update application configuration: ' + name)
        app_config[0].update(properties)
    else:
        print('create application configuration: ' + name)
        instance.create_application_configuration(name, properties, description)
    return name


def subscribe(topology, topic, schema, group=None, credentials=None, name=None):
    """Subscribe to messages from Event Streams (Message Hub) for a topic.

    Adds an Event Streams consumer that subscribes to a topic
    and converts each consumed message to a stream tuple.

    Args:
        topology(Topology): Topology that will contain the stream of messages.
        topic(str): Topic to subscribe messages from.
        schema(StreamSchema): Schema for returned stream.
        group(str): Kafka consumer group identifier. When not specified it default to the job name with `topic` appended separated by an underscore.
        credentials(dict|str): Credentials in JSON or name of the application configuration containing the credentials for the Event Streams service. When set to ``None`` the application configuration ``messagehub`` is used.
        name(str): Consumer name in the Streams context, defaults to a generated name.

    Returns:
         Stream: Stream containing messages.
    """
    if topic is None:
        raise TypeError(topic)
    msg_attr_name = None
    if schema is CommonSchema.Json:
        msg_attr_name = 'jsonString'
    elif schema is CommonSchema.String:
        msg_attr_name = 'string'
    elif schema is Schema.BinaryMessage:
        # msg_attr_name = 'message'
        pass
    elif schema is Schema.StringMessage:
        # msg_attr_name = 'message'
        pass
    elif schema is Schema.BinaryMessageMeta:
        # msg_attr_name = 'message'
        pass
    elif schema is Schema.StringMessageMeta:
        # msg_attr_name = 'message'
        pass
    else:
        raise ValueError(schema)

    if group is None:
        group = streamsx.spl.op.Expression.expression('getJobName() + "_" + "' + str(topic) + '"')

    if name is None:
        name = topic

    # check if it's the credentials for the service
    if isinstance(credentials, dict):
        appConfigName = None
    else:
        appConfigName = credentials

    _op = _MessageHubConsumer(topology, schema=schema, outputMessageAttributeName=msg_attr_name, appConfigName=appConfigName, topic=topic, groupId=group, name=name)
    if (appConfigName is None) and (credentials is not None):
        _op.params['messageHubCredentialsFile'] = _add_credentials_file(topology, credentials)

    return _op.stream


def publish(stream, topic, credentials=None, name=None):
    """Publish Event Streams messages to a topic.

    Adds an Event Streams producer where each tuple on `stream` is
    published as a message into IBM Event Streams cloud service.

    Args:
        stream(Stream): Stream of tuples to published as messages.
        topic(str): Topic to publish messages to.
        credentials(dict|str): Credentials in JSON or name of the application configuration containing the credentials for the Event Streams service. When set to ``None`` the application configuration ``messagehub`` is used.
        name(str): Producer name in the Streams context, defaults to a generated name.

    Returns:
        streamsx.topology.topology.Sink: Stream termination.
    """
    if topic is None:
        raise TypeError(topic)
    msg_attr_name = None
    streamSchema = stream.oport.schema
    if streamSchema == CommonSchema.Json:
        msg_attr_name = 'jsonString'
    elif streamSchema == CommonSchema.String:
        msg_attr_name = 'string'
    elif streamSchema is Schema.BinaryMessage:
        # msg_attr_name = 'message'
        pass
    elif streamSchema is Schema.StringMessage:
        # msg_attr_name = 'message'
        pass
    else:
        raise ValueError(streamSchema)

    # check if it's the credentials for the service
    if isinstance(credentials, dict):
        appConfigName = None
    else:
        appConfigName = credentials

    _op = _MessageHubProducer(stream, appConfigName=appConfigName, topic=topic, name=name)
    if (appConfigName is None) and (credentials is not None):
        _op.params['messageHubCredentialsFile'] = _add_credentials_file(stream.topology, credentials)

    # create the input attribute expressions after operator _op initialization
    if msg_attr_name is not None:
        _op.params['messageAttribute'] = _op.attribute(stream, msg_attr_name)
#    if keyAttributeName is not None:
#        params['keyAttribute'] = _op.attribute(stream, keyAttributeName)
#    if partitionAttributeName is not None:
#        params['partitionAttribute'] = _op.attribute(stream, partitionAttributeName)
#    if timestampAttributeName is not None:
#        params['timestampAttribute'] = _op.attribute(stream, timestampAttributeName)
#    if topicAttributeName is not None:
#        params['topicAttribute'] = _op.attribute(stream, topicAttributeName)

    return streamsx.topology.topology.Sink(_op)


class _MessageHubConsumer(streamsx.spl.op.Source):
    def __init__(self, topology, schema,
                 vmArg=None,
                 appConfigName=None,
                 clientId=None,
                 messageHubCredentialsFile=None,
                 outputKeyAttributeName=None,
                 outputMessageAttributeName=None,
                 outputTimestampAttributeName=None,
                 outputOffsetAttributeName=None,
                 outputPartitionAttributeName=None,
                 outputTopicAttributeName=None,
                 partition=None,
                 propertiesFile=None,
                 startPosition=None,
                 startTime=None,topic=None,
                 triggerCount=None,
                 userLib=None,
                 groupId=None,
                 name=None):
        kind = "com.ibm.streamsx.messagehub::MessageHubConsumer"
        # inputs = None
        schemas = schema
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
        super(_MessageHubConsumer, self).__init__(topology, kind, schemas, params, name)


class _MessageHubProducer(streamsx.spl.op.Sink):
    def __init__(self, stream,
                 vmArg=None,
                 appConfigName=None,
                 messageHubCredentialsFile=None,
                 propertiesFile=None,
                 topic=None,
                 userLib=None,
                 name=None):
        # topology = stream.topology
        kind = "com.ibm.streamsx.messagehub::MessageHubProducer"
        params = dict()
        if vmArg is not None:
            params['vmArg'] = vmArg
        if appConfigName is not None:
            params['appConfigName'] = appConfigName
        if messageHubCredentialsFile is not None:
            params['messageHubCredentialsFile'] = messageHubCredentialsFile
        if propertiesFile is not None:
            params['propertiesFile'] = propertiesFile
        if topic is not None:
            params['topic'] = topic
        if userLib is not None:
            params['userLib'] = userLib
        super(_MessageHubProducer, self).__init__(kind, stream, params, name)
