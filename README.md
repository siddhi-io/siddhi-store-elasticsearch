Siddhi Store Elasticsearch
=============================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-elasticsearch/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-elasticsearch/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-store-elasticsearch.svg)](https://github.com/siddhi-io/siddhi-store-elasticsearch/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-store-elasticsearch.svg)](https://github.com/siddhi-io/siddhi-store-elasticsearch/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-store-elasticsearch.svg)](https://github.com/siddhi-io/siddhi-store-elasticsearch/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-store-elasticsearch.svg)](https://github.com/siddhi-io/siddhi-store-elasticsearch/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-store-elasticsearch extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that persists and retrieve events to/from Elasticsearch

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 3.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.store.elasticsearch/siddhi-store-elasticsearch/">here</a>. This supports elastic search 7.x.x version and above.
* Versions 2.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.store.elasticsearch/siddhi-store-elasticsearch/">here</a>. This supports elastic search 6.x.x version.
* Versions 1.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.store.elasticsearch/siddhi-store-elasticsearch">here</a>. This supports elastic search 6.x.x version.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-elasticsearch/api/3.1.0">3.1.0</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-store-elasticsearch/api/3.1.0/#elasticsearch-store">elasticsearch</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#store">Store</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">Elasticsearch store implementation uses Elasticsearch indexing document for underlying data storage. The events are converted to Elasticsearch index documents when the events are inserted into the elasticsearch store. Elasticsearch indexing documents are converted to events when the documents are read from Elasticsearch indexes. The internal store is connected to the Elastisearch server via the Elasticsearch Java High Level REST Client library.</p></p></div>

## Prerequisites
 - Elasticsearch can be downloaded directly from href="https://www.elastic.co/downloads/elasticsearch" in zip, tar.gz, deb, or rpm packages. 
 - Then install the version 6.2.4, usual Ubuntu way with dpkg.
   
```
     sudo apt-get update
     wget https://download.elastic.co/elasticsearch/release/org/elasticsearch/distribution/deb/elasticsearch/6.2.4/elasticsearch-6.2.4.deb
     sudo dpkg -i elasticsearch-6.2.4.deb
```
   
   <a href="https://www.digitalocean.com/community/tutorials/how-to-install-and-configure-elasticsearch-on-ubuntu-16-04">How To Install and Configure Elasticsearch on Ubuntu 16.04</a>
 - Also you can start the Elasticsearch server using docker image:
   
```
     docker run -p 9600:9200 -p 9700:9300 -e "discovery.type=single-node" -e ELASTIC_PASSWORD=MagicWord -d docker.elastic.co/elasticsearch/elasticsearch:6.2.4
```
   
## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.

