# elasticsearch-osmosis-plugin

elasticsearch-osmosis-plugin is an [Osmosis](http://wiki.openstreetmap.org/wiki/Osmosis) plugin that inserts [OpenStreetMap](http://www.openstreetmap.org) data into an [elasticsearch](http://www.elasticsearch.org) cluster. It aims to help indexing the world, no more, no less :)

[![OpenStreetMap](https://raw.github.com/ncolomer/elasticsearch-osmosis-plugin/master/assets/openstreetmap.png)](http://www.openstreetmap.org)
[![elasticsearch](https://raw.github.com/ncolomer/elasticsearch-osmosis-plugin/master/assets/elasticsearch.png)](http://www.elasticsearch.org)

### Motivations

OpenStreetMap contributors are working hard to collect, store and maintain gigabytes of valuable geographical data (look at [there](http://live.openstreetmap.fr/) if you're not yet convinced!).
Among the tools created to serve these common goals, Osmosis is a powerful ETL-like Java application that eases importing / exporting / manipulate these data in a PostgreSQL relational database.

However, problems may appear when you want to query such large datasets under high load: RDBMS are known to not scale very well in such scenarios.
In addition, you are probably not interested in the richness of the OpenStreetMap data model or your needs are just simpler than what it offers.

Fortunately, since the rise of NoSQL databases, we now have other storage options. elasticsearch is one of these.
This promising search engine leverages the Java Lucene API to offer fast data indexing and querying over a distributed cluster.
In addition - and that's why we are here - it offers some greatful geo search features.

I started this project with the initial feeling of a natural combination between geographical data, a specialized ETL tool and a geo-enabled search engine.
This feeling remains to be demonstrated but first benchs makes me quite confident for the future :)

Here are the plugin's features:
* Create a general purpose index from OpenStreetMap data [core elements](http://wiki.openstreetmap.org/wiki/Elements) benefiting Osmosis [filtering capabilities](http://wiki.openstreetmap.org/wiki/Osmosis/Detailed_Usage)
* Create specialized indexes such as Reverse-Geocoding index (i.e. retrieve ways from a location) or Bounds index (i.e. retrieve the city/region/country from a location - still working on)

### Want to contribute? Things that would be great...

* Review elasticsearch mapping: I made choices, there's probably betters
* Implement new specialized index builders
* Handle OpenStreetMap changesets to update already indexed dumps
* And all the rest... any idea, issue report, bugfix or contribution is appreciated :)

### The dev's corner

The project is fully Mavenized. If you want to dig into the code, follow these 4 steps:

* ensure you have [Maven](http://maven.apache.org/) and [Git](http://git-scm.com/) installed
* clone the project somewhere locally
* run the `lib/install.sh` script. It will install `osmosis-core-0.41.jar` and `osmosis-xml-0.41.jar` in your local Maven repository as these artifacts are not available on the central
* import the project into your favorite Java editor 

From here, you can also run the test suite using `mvn clean test`: it includes inevitable unit tests and some portable (full in-memory) integration tests.

- - -

## 1. Installation

Osmosis installation is really easy and should not take you more than 5 minutes. Just follow/adapt the few shell commands below.
You might also be interested in the [osmosis-chef-cookbook](https://github.com/ncolomer/osmosis-chef-cookbook) that automates the osmosis installation on any Chef-managed node.

### 1.1. Install Osmosis

Untar the choosen build into the `/opt` directory (you can download them from the Osmosis builds [page](http://dev.openstreetmap.org/~bretth/osmosis-build/)), create the `/etc/osmosis` file and finally set the `$OSMOSIS_HOME` and `$PATH` environment variable accordingly.

5-shell-command procedure:

```shell
# Osmosis 0.41 installation
wget -P /tmp http://dev.openstreetmap.org/~bretth/osmosis-build/osmosis-0.41.tgz
tar -zxvf /tmp/osmosis-0.41.tgz -C /opt
echo "JAVACMD_OPTIONS=\"-server -Xmx2G\"" > /etc/osmosis # Put your JVM params there
export OSMOSIS_HOME=/opt/osmosis-0.41
export PATH=$PATH:$OSMOSIS_HOME/bin
```

### 1.2. Install elasticsearch-osmosis-plugin

Put the latest jar into `$OSMOSIS_HOME/lib/default` directory (see the [downloads](https://github.com/ncolomer/elasticsearch-osmosis-plugin/downloads) section) and add the `org.openstreetmap.osmosis.plugin.elasticsearch.elasticsearchWriterPluginLoader` line into the `$OSMOSIS_HOME/config/osmosis-plugins.conf` file (create it if necessary).

3-shell-command procedure:

```shell
# latest elasticsearch-osmosis-plugin 1.2.0 installation
wget -P /tmp https://github.com/downloads/ncolomer/elasticsearch-osmosis-plugin/elasticsearch-osmosis-plugin-1.2.0.jar
cp /tmp/elasticsearch-osmosis-plugin-1.2.0.jar $OSMOSIS_HOME/lib/default/
echo "org.openstreetmap.osmosis.plugin.elasticsearch.ElasticSearchWriterPluginLoader" > $OSMOSIS_HOME/config/osmosis-plugins.conf
```

## 2. Usage

### 2.1. Prerequisites

This plugin was built using elasticsearch 0.20.2 and Osmosis 0.41. It is not guaranteed to work with older version.
You must have an elasticsearch cluster up and running and reachable to make it running.

### 2.2. Plugin usage

To enable the plugin, append the following to your Osmosis command:

```
--write-elasticsearch (--wes)
```

Available options are:

<table>
	<tr>
		<th>Name</th><th>Type</th><th>Default value</th><th>Description</th>
	</tr>
	<tr>
		<td>hosts</td><td>String</td><td>localhost</td><td>Comma-separated list of nodes to join. 
			Valid syntax for a single node is <code>host1</code>, <code>host2:port</code> or <code>host3[portX-portY]</code></td>
	</tr>
	<tr>
		<td>clusterName</td><td>String</td><td>elasticsearch</td><td>Name of the elasticsearch cluster to join</td>
	</tr>
	<tr>
		<td>indexName</td><td>String</td><td>osm</td><td>Name of the index that will be filled with data</td>
	</tr>
	<tr>
		<td>createIndex</td><td>Boolean</td><td>true</td><td>(Re)create the main index (delete if exists!) and its mapping prior inserting data</td>
	</tr>
	<tr>
		<td>indexBuilders</td><td>String</td><td>[empty]</td><td>Comma-separated list of specialized index builder id (see below)</td>
	</tr>
</table>

### 2.3. Specialized index builders

Specialized indexes are custom and optimized representations of OpenStreetMap data. They allow you to execute queries that were not possible using the main index - the one that contains raw OpenStreetMap entities. Specialized indexe mapping takes advantage of elasticsearch advanced geo capabilities such as the [Geo Shape Type](http://www.elasticsearch.org/guide/reference/mapping/geo-shape-type.html).

Each built specialized index is accessible via its compound name `{indexName}-{indexBuilderId}`.

Available builders are:

<table>
	<tr>
		<th>Id</th><th>Name</th><th>Description</th>
	</tr>
	<tr>
		<td>rg</td><td>Reverse-Geocoding</td><td>This builder extracts all Ways containing the <code>highway</code> tag (whatever its value) from the main index. For each way, it gathers all its ordered nodes and finally map their locations into a <code>geo_shape</code> field as a <code>linestring</code>. Thus, you can use either GeoShape queries or GeoShape filters to retrieve any related Way from a location.</td>
	</tr>
</table>

### 2.4. Examples

Connect to cluster **elasticsearch** using default configuration:

```
osmosis \
	--read-pbf ~/osm/extract/guyane.osm.pbf \
	--write-elasticsearch
```

Connect to cluster named **openstreetmap** via Node `10.0.0.1:9310` 
and (re)create index **osm** prior to insert data:

```
osmosis \
	--read-pbf ~/osm/extract/guyane.osm.pbf \
	--wes hosts="10.0.0.1:9310" clustername="openstreetmap" createIndex="true"
```

## 3. Mapping

OSM data is organized in a relational model composed of [data primitives](https://wiki.openstreetmap.org/wiki/Data_Primitives) - mainly `node`, `way` and `relation` - linked each other by their `osmid`. As relational, this model fits well in a RDBMS (commonly PostgreSQL + Postgis) and is exportable. Even though XML is the official representation, OpenStreetMap also supports other compressed formats such as PBF (Protocol Buffers) or BZ2 (compressed XML). These files can be easily found on the Internet (see [4.1. Get some OSM test data](#41-get-some-osm-test-data)).

Osmosis is able to read both XML and PBF formats: it deserializes data into Java objects that can be processed through plugins.
In our case, the *elasticsearch-osmosis-plugin* will convert these Java objects into their JSON equivalent prior to be inserted into elasticsearch.

Please note that both user and version metadata are not inserted into elasticsearch for the moment.

Given the following `sample.osm` file:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<osm xmlns:xapi="http://jxapi.openstreetmap.org/" version="0.6" 
  generator="Osmosis SNAPSHOT-r26564" xapi:planetDate="2012-03-30T00:17:05Z">
  <node id="343866517" version="9" timestamp="2009-09-25T21:37:24Z" uid="149399" 
    user="awikatchikaen" changeset="2627737" lat="48.6752901" lon="2.379928"/>
  <node id="497017646" version="1" timestamp="2009-09-15T12:39:40Z" uid="149399" 
    user="awikatchikaen" changeset="2491394" lat="48.675636" lon="2.3795092"/>
  <node id="497017647" version="1" timestamp="2009-09-15T12:39:40Z" uid="149399" 
    user="awikatchikaen" changeset="2491394" lat="48.6755728" lon="2.3795936"/>
  <way id="40849832" version="1" timestamp="2009-09-15T12:39:40Z" uid="149399" 
    user="awikatchikaen" changeset="2491394">
    <nd ref="497017646"/>
    <nd ref="497017647"/>
    <nd ref="343866517"/>
    <tag k="highway" v="residential"/>
    <tag k="name" v="Avenue Marc Sangnier"/>
  </way>
</osm>
```

The *elasticsearch-osmosis-plugin* will convert and insert these data into elasticsearch using two different index's types:

* All **nodes** will be stored into the `node` type, with its `osmid` as elasticsearch `id`

```json
{"id":343866517,"location":[2.379928,48.6752901],"tags":{}}
{"id":497017646,"location":[2.3795092,48.675636],"tags":{}}
{"id":497017647,"location":[2.3795936,48.6755728],"tags":{}}
```

* All **ways** will be store into the `way` type, with its `osmid` as elasticsearch `id`

```json
{"id":40849832,"tags":{"highway":"residential","name":"Avenue Marc Sangnier"},"nodes":[497017646,497017647,343866517]}
```

* All **relations** and **bounds** (not present in this exmaple) are ignored because not yet implemented.

## 4. Tips box

### 4.1. Get some OSM test data

You can get OSM files (planet, extract) from various location. OpenStreetMap have some listed on their dedicated 
[Planet.osm](http://wiki.openstreetmap.org/wiki/Planet.osm) wiki page.
Here is an example on how to get the `guyane.osm.pbf` extract by [Geofabrik.de](http://www.geofabrik.de/):


```shell
mkdir -p ~/osm/extract ~/osm/planet ~/osm/output
wget -P ~/osm/extract http://download.geofabrik.de/openstreetmap/europe/france/guyane.osm.pbf
```

### 4.2. Useful elasticsearch HTTP commands

```shell
# Delete the whole osm index created by this plugin
curl -XDELETE 'http://localhost:9200/osm/'
# Retrieve a Node by id
curl -XGET 'http://localhost:9200/osm/node/343866517'
# Retrieve a Way by id
curl -XGET 'http://localhost:9200/osm/way/40849832'
```

## 5. External links

### 5.1. OpenStreetMap related

* [Statistics](http://wiki.openstreetmap.org/wiki/Stats) about OpenStreetMap database (number of users, amount of data, etc)

### 5.2. Osmosis related

* Osmosis detailed usage wiki [page](http://wiki.openstreetmap.org/wiki/Osmosis/Detailed_Usage)
* Self-Updating Local OpenStreetMap Extract [tutorial](https://docs.google.com/document/pub?id=1paaYsOakgJEYP380R70s4SGYq8ME3ASl-mweVi1DlQ4)
* Other Osmosis plugins (largely inspirated from)
  * Mapsforge's [mapsforge-map-writer](http://code.google.com/p/mapsforge/source/browse/trunk/mapsforge-map-writer/) plugin
  * Neo4j's [neo4j-osmosis-plugin](https://github.com/svzdvd/neo4j-osmosis-plugin/) plugin

### 5.3. elasticsearch related

* [cookbook-elasticsearch](https://github.com/karmi/cookbook-elasticsearch) by [karmi](https://github.com/karmi/)
* Advanced geospatial capabilities
  * Geo Location and Search [article](http://www.elasticsearch.org/blog/2010/08/16/geo_location_and_search.html)
  * Geo Point [Type](http://www.elasticsearch.org/guide/reference/mapping/geo-point-type.html), 
	[Distance Filter](http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-filter.html), 
	[Polygon Filter](http://www.elasticsearch.org/guide/reference/query-dsl/geo-polygon-filter.html), 
	[Bounding Box Filter](http://www.elasticsearch.org/guide/reference/query-dsl/geo-bounding-box-filter.html) and 
	[Distance Facets](http://www.elasticsearch.org/guide/reference/api/search/facets/geo-distance-facet.html), 
  * Geo Shape [Type](http://www.elasticsearch.org/guide/reference/mapping/geo-shape-type.html), 
	[Query](http://www.elasticsearch.org/guide/reference/query-dsl/geo-shape-query.html) and 
	[Filter](http://www.elasticsearch.org/guide/reference/query-dsl/geo-shape-filter.html)

