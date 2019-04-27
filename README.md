Siddhi-store-elasticsearch
======================================

The **siddhi-store-elasticsearch extension** is an extension for siddhi Elasticsearch event table implementation. This extension can be used to persist events to a
Elasticsearch server instance of version 6.x.x.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-elasticsearch">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-elasticsearch/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-elasticsearch/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-elasticsearch/api/1.1.3">1.1.3</a>.

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
 
## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-elasticsearch/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>org.wso2.extension.siddhi.store.elasticsearch</groupId>
        <artifactId>siddhi-store-elasticsearch</artifactId>
        <version><version>x.x.x</version></version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-elasticsearch/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-store-elasticsearch/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-store-elasticsearch/api/1.1.3/#elasticsearch-store">elasticsearch</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#store">(Store)</a>*<br><div style="padding-left: 1em;"><p>Elasticsearch store implementation uses Elasticsearch indexing document for underlying data storage. The events are converted to Elasticsearch index documents when the events are inserted into the elasticsearch store. Elasticsearch indexing documents are converted to events when the documents are read from Elasticsearch indexes. The internal store is connected to the Elastisearch server via the Elasticsearch Java High Level REST Client library.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-elasticsearch/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-store-elasticsearch/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 
