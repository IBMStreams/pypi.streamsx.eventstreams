from unittest import TestCase

import streamsx.eventstreams as evstr
from streamsx.eventstreams.schema import Schema as MsgSchema
from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
from streamsx.topology.schema import CommonSchema, StreamSchema

import streamsx.spl.toolkit

import datetime
import os
import time
import uuid
import json
from streamsx.rest import StreamingAnalyticsConnection

##
## Test assumptions
## ----------------
##
## Streaming analytics service has:
##    application config 'messagehub' configured for Eventstreams service
##
## MessageHub toolkit can be found in path ${EVENTSTREAMS_TOOLKIT_HOME} and is at least 1.5.1
##
## Event Streams service has:
##
##    topic MH_TEST with one partition (1 hour retention)
##
## Credentials are stored in a file denoted by $EVENTSTREAMS_CREDENTIALS environment variable

class TestSubscribeParams(TestCase):
    def test_schemas_ok(self):
        topo = Topology()
        evstr.subscribe(topo, 'T1', CommonSchema.String)
        evstr.subscribe(topo, 'T1', CommonSchema.Json)
        evstr.subscribe(topo, 'T1', MsgSchema.StringMessage)
        evstr.subscribe(topo, 'T1', MsgSchema.BinaryMessage)
        evstr.subscribe(topo, 'T1', MsgSchema.StringMessageMeta)
        evstr.subscribe(topo, 'T1', MsgSchema.BinaryMessageMeta)

    def test_schemas_bad(self):
        topo = Topology()
        self.assertRaises(TypeError, evstr.subscribe, topo, 'T1', CommonSchema.Python)
        self.assertRaises(TypeError, evstr.subscribe, topo, 'T1', CommonSchema.Binary)
        self.assertRaises(TypeError, evstr.subscribe, topo, 'T1', CommonSchema.XML)
        self.assertRaises(TypeError, evstr.subscribe, topo, 'T1', StreamSchema('tuple<int32 a>'))
        self.assertRaises(TypeError, evstr.subscribe, topo, 'T1', 'tuple<int32 a>')

    def test_creds(self):
        creds_file = os.environ['EVENTSTREAMS_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology()
        evstr.subscribe(topo, 'T1', CommonSchema.String, credentials=credentials)
        evstr.subscribe(topo, 'T1', CommonSchema.String, credentials='eventstreams')

class TestPublishParams(TestCase):
    def test_schemas_ok(self):
        topo = Topology()
        pyObjStream = topo.source(['Hello', 'World!'])
        jsonStream = pyObjStream.as_json()
        stringStream = pyObjStream.as_string()
        binMsgStream = pyObjStream.map (func=lambda s: {'message': bytes(s, 'utf-8'), 'key': s}, schema=MsgSchema.BinaryMessage)
        strMsgStream = pyObjStream.map (func=lambda s: {'message': s, 'key': s}, schema=MsgSchema.StringMessage)
        evstr.publish (binMsgStream, "Topic")
        evstr.publish (strMsgStream, "Topic")
        evstr.publish (stringStream, "Topic")
        evstr.publish (jsonStream, "Topic")

    def test_schemas_bad(self):
        topo = Topology()
        pyObjStream = topo.source(['Hello', 'World!'])
        binStream = pyObjStream.map (func=lambda s: bytes ("ABC", utf-8), schema=CommonSchema.Binary)
        xmlStream = pyObjStream.map (schema=CommonSchema.XML)
        binMsgMetaStream = pyObjStream.map (func=lambda s: {'message': bytes(s, 'utf-8'), 'key': s}, schema=MsgSchema.BinaryMessageMeta)
        strMsgMetaStream = pyObjStream.map (func=lambda s: {'message': s, 'key': s}, schema=MsgSchema.StringMessageMeta)
        otherSplTupleStream1 = pyObjStream.map (schema=StreamSchema('tuple<int32 a>'))
        otherSplTupleStream2 = pyObjStream.map (schema='tuple<int32 a>')
        
        self.assertRaises(TypeError, evstr.publish, pyObjStream, "Topic")
        self.assertRaises(TypeError, evstr.publish, binStream, "Topic")
        self.assertRaises(TypeError, evstr.publish, xmlStream, "Topic")
        self.assertRaises(TypeError, evstr.publish, binMsgMetaStream, "Topic")
        self.assertRaises(TypeError, evstr.publish, strMsgMetaStream, "Topic")
        self.assertRaises(TypeError, evstr.publish, otherSplTupleStream1, "Topic")
        self.assertRaises(TypeError, evstr.publish, otherSplTupleStream2, "Topic")

    def test_creds(self):
        creds_file = os.environ['EVENTSTREAMS_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology()
        stream = topo.source (['Hello', 'World']).as_json()
        evstr.publish (stream, 'Topic', credentials=credentials)
        evstr.publish (stream, 'Topic', credentials='eventstreams')

## Using a uuid to avoid concurrent test runs interferring
## with each other
class JsonData(object):
    def __init__(self, prefix, count, delay=True):
        self.prefix = prefix
        self.count = count
        self.delay = delay
    def __call__(self):
        # Since we are reading from the end allow
        # time to get the consumer started.
        if self.delay:
            time.sleep(10)
        for i in range(self.count):
            yield {'p': self.prefix + '_' + str(i), 'c': i}

class StringData(object):
    def __init__(self, prefix, count, delay=True):
        self.prefix = prefix
        self.count = count
        self.delay = delay
    def __call__(self):
        if self.delay:
            time.sleep(10)
        for i in range(self.count):
            yield self.prefix + '_' + str(i)

def get_toolkit_home():
    result = None
    try:
        result = os.environ['EVENTSTREAMS_TOOLKIT_HOME']
    except KeyError: 
        result = None
    return result

def add_mh_toolkit(topo):
    if get_toolkit_home() is not None:
        streamsx.spl.toolkit.add_toolkit(topo, get_toolkit_home())


class TestMH(TestCase):
    def setUp(self):
      remoteBuild = True
      Tester.setup_streaming_analytics (self, force_remote_build=remoteBuild)

    def test_json(self):
        n = 104
        topo = Topology()
        add_mh_toolkit(topo)
        uid = str(uuid.uuid4())
        s = topo.source(JsonData(uid, n)).as_json()
        evstr.publish(s, 'MH_TEST')
        print ('test_json')

        r = evstr.subscribe(topo, 'MH_TEST', CommonSchema.Json)
        r = r.filter(lambda t : t['p'].startswith(uid))
        expected = list(JsonData(uid, n, False)())

        tester = Tester(topo)
        tester.contents(r, expected)
        tester.tuple_count(r, n)
        tester.test(self.test_ctxtype, self.test_config)

    def test_string(self):
        n = 107
        topo = Topology()
        add_mh_toolkit(topo)
        uid = str(uuid.uuid4())
        s = topo.source(StringData(uid, n)).as_string()
        evstr.publish(s, 'MH_TEST')
        print ('test_string')

        r = evstr.subscribe(topo, 'MH_TEST', CommonSchema.String)
        r = r.filter(lambda t : t.startswith(uid))
        expected = list(StringData(uid, n, False)())

        tester = Tester(topo)
        tester.contents(r, expected)
        tester.tuple_count(r, n)
        tester.test(self.test_ctxtype, self.test_config)

    def test_string_creds(self):
        n = 107
        creds_file = os.environ['EVENTSTREAMS_CREDENTIALS']
        with open(creds_file) as data_file:
            credentials = json.load(data_file)
        topo = Topology()
        add_mh_toolkit(topo)
        uid = str(uuid.uuid4())
        s = topo.source(StringData(uid, n)).as_string()
        print ('test_string_creds')
        evstr.publish(s, 'MH_TEST', credentials=credentials)

        r = evstr.subscribe(topo, 'MH_TEST', CommonSchema.String, credentials=credentials)
        r = r.filter(lambda t : t.startswith(uid))
        expected = list(StringData(uid, n, False)())

        tester = Tester(topo)
        tester.contents(r, expected)
        tester.tuple_count(r, n)
        tester.test(self.test_ctxtype, self.test_config)

#    def test_create_app_config(self):
#        n = 107
#        creds_file = os.environ['EVENTSTREAMS_CREDENTIALS']
#        with open(creds_file) as data_file:
#            credentials = json.load(data_file)
#            
#        print (credentials)
#        streamingAnalyticsCon = StreamingAnalyticsConnection ()
#        instance = streamingAnalyticsCon.get_instances()[0]
#        app_config_name = evstr.configure_connection (instance, credentials = credentials)
#        print (app_config_name)
#        topo = Topology()
#        add_mh_toolkit(topo)
#        uid = str(uuid.uuid4())
#        s = topo.source(StringData(uid, n)).as_string()
#        print ('test_string_creds')
#        evstr.publish(s, 'MH_TEST', credentials='messagehub')

#        r = evstr.subscribe(topo, 'MH_TEST', CommonSchema.String, credentials='messagehub')
#        r = r.filter(lambda t : t.startswith(uid))
#        expected = list(StringData(uid, n, False)())

#        tester = Tester(topo)
#        tester.contents(r, expected)
#        tester.tuple_count(r, n)
#        tester.test(self.test_ctxtype, self.test_config)

class TestICPRemote(TestMH):
    def setUp(self):
        Tester.setup_distributed(self)
        self.toolkit_home = None
        # setup test config
        self.test_config = {}
        job_config = streamsx.topology.context.JobConfig(tracing='info')
        job_config.add(self.test_config)
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False 

    @classmethod
    def setUpClass(self):
        super().setUpClass()
