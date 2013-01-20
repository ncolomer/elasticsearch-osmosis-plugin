# elasticsearch-osmosis-plugin

elasticsearch-osmosis-plugin is an Osmosis plugin that allows insert OpenStreetMap data into an elasticsearch cluster.
Its main purpose is to index the world, literally :-)

- - -

## 1. Installation

Osmosis installation is really easy and should not take you more than 5 minutes. Just follow/adapt the few shell commands below :-)
You could also be interested in the [osmosis-chef-cookbook](https://github.com/ncolomer/osmosis-chef-cookbook) that automates the osmosis installation on any Chef-managed node.

### 1.1. Install Osmosis

Untar the wanted build (look at the Osmosis builds [page](http://dev.openstreetmap.org/~bretth/osmosis-build/)) into 
the <code>/opt</code> directory, create the <code>/etc/osmosis</code> file and set the <code>$OSMOSIS_HOME</code> and 
<code>$PATH</code> environment variable accordingly.

5-shell-command procedure:

	# Osmosis 0.41 installation
	wget -P /tmp http://dev.openstreetmap.org/~bretth/osmosis-build/osmosis-0.41.tgz
	tar -zxvf /tmp/osmosis-0.41.tgz -C /opt
    echo "JAVACMD_OPTIONS=\"-server -Xmx2G\"" > /etc/osmosis
    export OSMOSIS_HOME=/opt/osmosis-0.41
    export PATH=$PATH:$OSMOSIS_HOME/bin

### 1.2. Install elasticsearch-osmosis-plugin

Put the latest jar (see the [downloads](https://github.com/ncolomer/elasticsearch-osmosis-plugin/downloads) section) 
into <code>$OSMOSIS_HOME/lib/default</code> directory and add the <code>org.openstreetmap.osmosis.plugin.elasticsearch.elasticsearchWriterPluginLoader</code>
line into the <code>$OSMOSIS_HOME/config/osmosis-plugins.conf</code> file (create it if necessary).

3-shell-command procedure:

	# elasticsearch-osmosis-plugin 1.1.0 installation
    wget -P /tmp https://github.com/downloads/ncolomer/elasticsearch-osmosis-plugin/elasticsearch-osmosis-plugin-1.1.0.jar
    cp /tmp/elasticsearch-osmosis-plugin-1.1.0.jar $OSMOSIS_HOME/lib/default/
    echo "org.openstreetmap.osmosis.plugin.elasticsearch.ElasticSearchWriterPluginLoader" > $OSMOSIS_HOME/config/osmosis-plugins.conf

## 2. Usage

### 2.1. Prerequisites

This plugin was built using elasticsearch 0.20.2 and Osmosis 0.41. It is not guaranteed to work with older version.
You must have an elasticsearch cluster up and running and reachable to make it running.

### 2.2. Plugin usage

To enable the plugin, append the following to your Osmosis command:

    --write-elasticsearch (--wes)

Available options are:

<table>
	<tr>
		<th>Name</th><th>Type</th><th>Default value</th><th>Description</th>
	</tr>
	<tr>
		<td>clusterName</td><td>String</td><td>elasticsearch</td><td>Name of the elasticsearch cluster to join</td>
	</tr>
	<tr>
		<td>isNodeClient</td><td>Boolean</td><td>true</td><td>Join as NodeClient or TransportClient 
			(See <a href="http://www.elasticsearch.org/guide/reference/java-api/client.html">here</a> for the difference)</td>
	</tr>
	<tr>
		<td>host</td><td>String</td><td>localhost</td><td>Hostname or IP of the elasticsearch node to join</td>
	</tr>
	<tr>
		<td>port</td><td>Integer</td><td>9300</td><td>Transport port of the elasticsearch node to join</td>
	</tr>
	<tr>
		<td>indexName</td><td>String</td><td>osm</td><td>Name of the index that will be filled with data</td>
	</tr>
	<tr>
		<td>createIndex</td><td>Boolean</td><td>false</td><td>(Re)create (delete if exists!) the index before inserts</td>
	</tr>	
</table>

### 2.3. Examples

Connect to cluster **elasticsearch** as <code>NodeClient</code> through <code>localhost:9300</code>:

    osmosis \
        --read-pbf ~/osm/extract/guyane.osm.pbf \
        --write-elasticsearch

Connect to cluster **openstreetmap** as <code>TransportClient</code> through <code>10.0.0.1:9300</code> 
and (re)create index **osm** prior to insert data:

    osmosis \
    	--read-pbf ~/osm/extract/guyane.osm.pbf \
    	--wes isNodeClient="false" host="10.0.0.1" clustername="openstreetmap" createIndex="true"

## 3. Mapping

OSM data is organized in a relational model composed of [data primitives](https://wiki.openstreetmap.org/wiki/Data_Primitives) - mainly <code>node</code>, <code>way</code> and <code>relation</code> - linked each other by their <code>osmid</code>. As relational, this model fits well in a RDBMS (commonly PostgreSQL + Postgis) and is exportable. Even though XML is the official representation, OpenStreetMap also supports other compressed formats such as PBF (Protocol Buffers) or BZ2 (compressed XML). These files can be easily found on the Internet (see [4.1. Get some OSM test data](#41-get-some-osm-test-data)).

The Osmosis tool is able to read both XML and PBF formats: it deserializes data into Java objects that can be processed through plugins.
In our case, the *elasticsearch-osmosis-plugin* will convert these Java objects into their JSON equivalent prior to be inserted into elasticsearch.

Please note that all user and version metadata are not inserted into elasticsearch for the moment.

Given the following <code>sample.osm</code> file:

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

* All **nodes** will be stored into the <code>node</code> type, with its <code>osmid</code> as elasticsearch <code>id</code>

```json
{"id":343866517,"location":[2.379928,48.6752901],"tags":{}}
{"id":497017646,"location":[2.3795092,48.675636],"tags":{}}
{"id":497017647,"location":[2.3795936,48.6755728],"tags":{}}
```

Execute the following command to retrieve the first node above:

```
curl -XGET 'http://localhost:9200/osm/node/343866517'
```

* All **ways** will be store into the <code>way</code> type, with its <code>osmid</code> as elasticsearch <code>id</code>

```json
{"id":40849832,"tags":{"highway":"residential","name":"Avenue Marc Sangnier"},"nodes":[497017646,497017647,343866517]}
```

Execute the following command to retrieve the way above:

```
curl -XGET 'http://localhost:9200/osm/way/40849832'
```

* All **relations** and **bounds** (not present in this exmaple) are ignored because not yet implemented.

## 4. Tips box

### 4.1. Get some OSM test data

You can get OSM files (planet, extract) from various location. OpenStreetMap have some listed on their dedicated 
[Planet.osm](http://wiki.openstreetmap.org/wiki/Planet.osm) wiki page.
Here is an example on how to get the <code>guyane.osm.pbf</code> extract by [Geofabrik.de](http://www.geofabrik.de/):

    mkdir -p ~/osm/extract ~/osm/planet ~/osm/output
    wget -P ~/osm/extract http://download.geofabrik.de/openstreetmap/europe/france/guyane.osm.pbf

### 4.2. Useful elasticsearch HTTP commands

    # Reset the whole osm index created by this plugin
    curl -XDELETE 'http://localhost:9200/osm/'

## 5. Resources

### 5.1. Osmosis related

* Osmosis detailed usage wiki [page](http://wiki.openstreetmap.org/wiki/Osmosis/Detailed_Usage)
* Self-Updating Local OpenStreetMap Extract [tutorial](https://docs.google.com/document/pub?id=1paaYsOakgJEYP380R70s4SGYq8ME3ASl-mweVi1DlQ4)
* Other Osmosis plugins (largely inspirated from)
  * Mapsforge's [mapsforge-map-writer](http://code.google.com/p/mapsforge/source/browse/trunk/mapsforge-map-writer/) plugin
  * Neo4j's [neo4j-osmosis-plugin](https://github.com/svzdvd/neo4j-osmosis-plugin/) plugin

### 5.2. elasticsearch related

* [elasticsearch Chef cookbook](https://github.com/karmi/cookbook-elasticsearch) by [karmi](https://github.com/karmi/)
* Documentation about geospatial capabilities
  * Geo Location and Search [article](http://www.elasticsearch.org/blog/2010/08/16/geo_location_and_search.html)
  * Geo Distance Filter [guide](http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-filter.html)
  * Geo Point Type [guide](http://www.elasticsearch.org/guide/reference/mapping/geo-point-type.html)
