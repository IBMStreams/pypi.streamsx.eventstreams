## Python streamsx.eventstreams package.

This exposes SPL operators in the `com.ibm.streamsx.messagehub` toolkit as Python methods.

Package is organized using standard packaging to upload to PyPi.

The package is uploaded to PyPi in the standard way:
```
cd package
rm -rf streamsx.eventstreams.egg-info/ build/ dist/
python setup.py sdist bdist_wheel upload -r pypi
```
**Note:** This is done using the `ibmstreams` account at pypi.org

Package details: https://pypi.python.org/pypi/streamsx.eventstreams

Documentation is using Sphinx and can be built locally using:
```
cd package/docs
make html
```
and viewed using
```
firefox build/html/index.html
```

The documentation is also setup at `readthedocs.io` under the account: `IBMStreams`

Documentation links:
* http://streamsxeventstreams.readthedocs.io

## Version update

To change the version information of the Python package, edit following files:

- ./package/docs/source/conf.py
- ./package/streamsx/eventstreams/\_\_init\_\_.py

When the development status changes, edit the *classifiers* in

- ./package/setup.py

When the documented sample must be changed, change it here:

- ./package/streamsx/eventstreams/\_\_init\_\_.py
- ./package/DESC.txt


## Test

Package can be tested with TopologyTester using the Streaming Analytics service and Event Streams service on IBM Cloud.
Use Python 3.5 for tests with Streaming Analytics!

| Environment variable | content |
| --- | --- |
| VCAP_SERVICES | must point to a file containing VCAP information |
| STREAMING_ANALYTICS_SERVICE_NAME | the name of your Streaming Analytics Service |
| STREAMS_INSTALL | must point to your Streams installation, only required for local build |
| EVENTSTREAMS_TOOLKIT_HOME | The directory where the MessageHub toolkit is located |
| EVENTSTREAMS_CREDENTIALS | The name of a JSON file with Event Streams service credentials |
| JAVA_HOME | Java Home, at least Java 1.8, required for remote build |


Uninstall the `streamsx.eventstreams` package from your Python environment before test:

`pip uninstall streamsx.eventstreams --yes`


For the tests, an application configuration with name `messagehub` is required. It must contain the
Event Streams service credentials as `messagehub.creds` property.

For tests with credentials as dictionary, the environment variable `EVENTSTREAMS_CREDENTIALS` must exist.
In the Event Streams service, the topic `MH_TEST` with a single partition must be created.

Run the tests with

```
cd package
python3 -u -m unittest streamsx.eventstreams.tests.test_eventstreams.TestMH.test_json   # only test_json
python3 -u -m unittest streamsx.eventstreams.tests.test_eventstreams                    # all tests
```
