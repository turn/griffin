# Griffin
Griffin is a library to replicate a blob of data to a cluster of machines spanning across
multiple data centers.

Griffin was developed at [Turn Inc.](http://www.turn.com/) where it was used to replicate data across
machines in a single data center. At Turn, Griffin was tested in a production setting and was found to be able to
replicate data
- Of sizes ranging from a few megabytes to up to a gigabytes (compressed size)
- To a couple of hundred machines in a single data center
- With a latency of couple of minutes (time from data being available at any node to the time when data was available
  at all the nodes in the destination group)

While Griffin has been designed and well tested (in a docker environment) for replication across multiple data centers it
has never been used in a production setting across **multiple** data centers.

## Quickstart with Docker
**Requires**
- Docker 1.8+
- Docker compose (https://docs.docker.com/compose/install/)

### Starting a full cluster using docker-compose
A `docker-compose.yml` to create a full cluster of zookeeper, kafka, and griffin is included in this repository. To
create a cluster, update `KAFKA_ADVERTISED_HOST_NAME` in `docker-compose.yml` to the IP address of the host machine
(linux box) and start the cluster as
```sh
docker-compose up -d
docker-compose scale kafkalax1=3
docker-compose scale griffinlax1=3
```
This might take some time as it requires docker to download few images.

The above steps starts a zookeeper node, a Apache Kafka cluster of three brokers, and cluster of three nodes running
Griffin. To get the blob of data (or files) available locally at each of the Griffin nodes
```sh
for i in $(seq 3)
do
  curl -i http://$(docker port griffin_griffinlax1_$i 8080)/griffin/localrepo
done
```
Since we have not pushed any data via griffin yet the output of the above command would be empty

We will now upload a file(`build.gradle`) to one of the griffin node, namely `griffin_griffinlax1_1`, and then ask griffin to
replicate it to all the other griffin nodes (`dest='.*'`). We would like this file to be named `gradle`
(`blobname=gradle`) on all the griffin nodes.
```sh
curl -i# -F blobname=gradle -F dest='.*' -F file=@build.gradle http://$(docker port griffin_griffinlax1_1 8080)/griffin/localrepo
```

We can now observe (after a short delay) that the file is available at all griffin nodes
```sh
for i in $(seq 3)
do
  curl -i http://$(docker port griffin_griffinlax1_$i 8080)/griffin/localrepo
done
```

## REST API
### GET /localrepo
List all the files in the local repository of that instance of griffin

### PUT,POST /localrepo/
Push a given file to the specified set of machines

|Parameter|Type|Required|Description|
|---------|----|--------|-----------|
|blobname|string|true|A name by which this blob of bytes will be referred by the repository |
|dest|string|true|A regular expression specifying the destination. This blob will be downloaded by any server whose ```serverid``` matches the regex|
|file|form|true|Multi-part content specifying the data bytes|


### GET /globalrepo
List all the files available in all the instances of griffin  (henceforth called global repository)

### PUT,POST /globalrepo/
Push a given file to the specified set of machines

|Parameter|Type|Required|Description|
|---------|----|--------|-----------|
|blobname|string|true|A name by which this blob of bytes will be referred by the repository |
|dest|string|true|A regular expression specifying the destination. This blob will be downloaded by any server whose ```serverid``` matches the regex|
|file|form|true|Multi-part content specifying the data bytes|

### GET /missing
List all the files in global repository that is supposed to be in the local repository but has not
yet been download


## Design Rationale
TODO: _Explain the design space and the design choices_

## Design Deep Dive
TODO: _Component design and description_


## Running Griffin as a stand alone service

### Build

#### Requirements
- Gradle 2.5+
- protoc 2.5+
- Java 1.7

Prepare a single griffin fat jar with
```sh
gradle build
```
Fat jar is available at `./build/libs/griffin-*.jar`. Include this jar in your application's library folder to use griffin.


### Running Griffin
#### Required configuration
##### Griffin
Griffin requires a configuration file `griffin.conf` in the local directory. A sample `griffin.conf` is included in the
`examples` directory. Please update the file with the required information.

- Griffin.ZkServers: `LIST_OF_ZKSERVERS`
- Griffin.KafkaBrokersList: `LIST_OF_KAFKA_BROKERS_WITH_PORTS`
- Griffin.Error.Email.Recipients: `EMAIL_ID`
- serverid: `UNIQUE_SERVER_ID`
- dcname: `DATACENTER_NAME`

##### Apache Kafka
Griffin requires that Apache Kafka has automatic topic creation and topic deletion enabled, i.e.,
```sh
auto.create.topics.enable=true
delete.topic.enable=true
```
#### Griffin Example
We will run two instances of griffin. Each instance of griffin is required to have a unique `serverid`. Zookeeper
cluster specified using `ZkServers` and Kafka cluster specified using `KafkaBrokersList` should be same for all griffin
services that share the same `dcname`

Open two terminals. In first terminal start the first instance of griffin as
```sh
cd examples
java -jar ../build/libs/griffin-*.jar
```

In second terminal change `serverid` in `examples/griffin.conf` to `griffin-2` and `server.port` in
`examples/application.properties` to `8081` and start another instance of griffin as
```sh
cd examples
java -jar ../build/libs/griffin-*.jar
```

In yet another terminal we can interact with these griffin services. We will push a file to one instance of griffin and
we will observe it getting replicated to another instance of griffin.
First we can observe that both instances for griffin does not contain any file in their local repository
```sh
curl -i http://localhost:8080/griffin/localrepo/
curl -i http://localhost:8081/griffin/localrepo/
```

We can push a file to one of the griffin instances as
```sh
curl -i -F blobname=gradle -F dest='.*' -F file=@build.gradle http://localhost:8080/griffin/localrepo
```

After a short while one can observe the file available in local repository of both griffin services
```sh
curl -i http://localhost:8080/griffin/localrepo/
curl -i http://localhost:8081/griffin/localrepo/
```
Check out REST API section for full description of rest calls above.


## Javadoc
```sh
gradle javadoc
```
Javadocs are generated in `./build/docs/javadoc/index.html`.



## Notes
- The first incarnation of Griffin was written for Java 6. So while the current version of Griffin
requires Java 7, it does not fully leverage all the new features of Java 7.


