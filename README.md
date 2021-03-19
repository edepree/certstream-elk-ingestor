# certstream-elk-ingestor

A transport script for moving data from a certstream server to Elasticsearch instance. Due to issue with the latest version of the `websocket-client` library (as of today) the library is pinned at a known good version within the included Pipfile.

Some useful commands learned during development of this were:
* pipenv install certstream
* pipenv shell