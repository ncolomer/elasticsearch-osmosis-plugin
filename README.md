# elasticsearch-osmosis-plugin

An Osmosis plugin that inserts OpenStreetMap data into an Elasticsearch cluster

- - -

## 1. Installation

### 1.1 Install Osmosis

Untar the wanted build (look at the Osmosis builds [page](http://dev.openstreetmap.org/~bretth/osmosis-build/)) into 
the <code>/opt</code> directory, create the <code>/etc/osmosis</code> file and set the <code>$OSMOSIS_HOME</code> and 
<code>$PATH</code> environment variable accordingly.

5 shell command procedure:

    # Osmosis 0.40.1 installation
    wget -P /tmp http://dev.openstreetmap.org/~bretth/osmosis-build/osmosis-0.40.1.tgz
    tar -zxvf /tmp/osmosis-0.40.1.tgz -C /opt
    echo "JAVACMD_OPTIONS=\"-server -Xmx2G\"" > /etc/osmosis
    export OSMOSIS_HOME=/opt/osmosis-0.40.1
    export PATH=$PATH:$OSMOSIS_HOME/bin
    
You may also be interested in the [osmosis-chef-cookbook](https://github.com/ncolomer/osmosis-chef-cookbook) that 
automates the osmosis installation on a Chef-managed node.

### 1.2 Install elasticsearch-osmosis-plugin

Put the latest jar (see the [downloads](https://github.com/ncolomer/elasticsearch-osmosis-plugin/downloads) section) 
into <code>$OSMOSIS_HOME/lib/default</code> directory and add the <code>org.openstreetmap.osmosis.plugin.elasticsearch.ElasticSearchWriterPluginLoader</code>
line into the <code>$OSMOSIS_HOME/config/osmosis-plugins.conf</code> file (create it if necessary).

3 shell command procedure:

    # elasticsearch-osmosis-plugin 0.0.2 installation
    wget -P /tmp https://github.com/downloads/ncolomer/elasticsearch-osmosis-plugin/elasticsearch-osmosis-plugin-0.0.2.jar
    cp /tmp/elasticsearch-osmosis-plugin-0.0.2.jar $OSMOSIS_HOME/lib/default/
    echo "org.openstreetmap.osmosis.plugin.elasticsearch.ElasticSearchWriterPluginLoader" > $OSMOSIS_HOME/config/osmosis-plugins.conf

## 2. Usage

### 2.1 Prerequisites

You must have an Elasticsearch cluster up and running and reachable to make this plugin running.

### 2.2 Download

You can get osm files (planet, extract) from various location. OpenStreetMap have some listed on their dedicated 
[Planet.osm](http://wiki.openstreetmap.org/wiki/Planet.osm) wiki page.

Here is an example for the <code>ile-de-france.osm.pbf</code> extract by [Geofabrik.de](http://www.geofabrik.de/):

    mkdir -p ~/osm/extract ~/osm/planet ~/osm/output
    wget -P ~/osm/extract http://download.geofabrik.de/osm/europe/france/ile-de-france.osm.pbf

### 2.3 Plugin usage

    --write-elasticsearch (--wes)
    Options:
      host (default: localhost)
      port (default: 9300)
      clustername (default: elasticsearch)

### 2.4 Examples

    osmosis \
    	--read-pbf ~/osm/extract/ile-de-france.osm.pbf \
    	--way-key keyList="highway" \
    	--tag-filter reject-relations \
    	--used-node \
    	--write-elasticsearch host="10.0.0.1" clustername="mycluster"

## 3. Administration

Some useful HTTP command:

    # Reset the whole osm index created by this plugin
    curl -XDELETE 'http://localhost:9200/osm/'

## 4. Resources

### Osmosis related

* Osmosis detailed usage wiki [page](http://wiki.openstreetmap.org/wiki/Osmosis/Detailed_Usage)
* Self-Updating Local OpenStreetMap Extract [tutorial](https://docs.google.com/document/pub?id=1paaYsOakgJEYP380R70s4SGYq8ME3ASl-mweVi1DlQ4)
* Other Osmosis plugins (largely inspirated from)
  * Mapsforge's [mapsforge-map-writer](http://code.google.com/p/mapsforge/source/browse/trunk/mapsforge-map-writer/) plugin
  * Neo4j's [neo4j-osmosis-plugin](https://github.com/svzdvd/neo4j-osmosis-plugin/) plugin

### Elasticsearch related

* [Elasticsearch Chef cookbook](https://github.com/karmi/cookbook-elasticsearch) by [karmi](https://github.com/karmi/)
* Documentation about geospatial capabilities
  * Geo Location and Search [article](http://www.elasticsearch.org/blog/2010/08/16/geo_location_and_search.html)
  * Geo Distance Filter [guide](http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-filter.html)
  * Geo Point Type [guide](http://www.elasticsearch.org/guide/reference/mapping/geo-point-type.html)
