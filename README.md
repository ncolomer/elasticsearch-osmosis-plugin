# elasticsearch-osmosis-plugin
An Osmosis plugin that injects OSM data into an Elasticsearch cluster

- - -

## 1. Bootstrap

### 1.1 Install Osmosis

Follow this procedure:

    # Osmosis 0.40.1 installation
    wget -P /tmp http://dev.openstreetmap.org/~bretth/osmosis-build/osmosis-0.40.1.tgz
    tar -zxvf /tmp/osmosis-0.40.1.tgz -C /opt
    echo "JAVACMD_OPTIONS=\"-server -Xmx2G\"" > /etc/osmosis
    export OSMOSIS_HOME=/opt/osmosis-0.40.1
    export PATH=$PATH:$OSMOSIS_HOME/bin

### 1.2 Install elasticsearch-osmosis-plugin

Put jar into <code>lib/default</code> and add line <code>org.openstreetmap.osmosis.plugin.elasticsearch.ElasticSearchWriterPluginLoader</code> 
into the <code>config/osmosis-plugins.conf</code> file (create if necessary).

    cp ~/elasticsearch-plugin-0.0.1-SNAPSHOT.jar $OSMOSIS_HOME/lib/default/
    echo "org.openstreetmap.osmosis.plugin.elasticsearch.ElasticSearchWriterPluginLoader" > $OSMOSIS_HOME/config/osmosis-plugins.conf

### 1.3 Download

You can get osm files from the [download](http://download.geofabrik.de/) section of [geofabrik.de](http://www.geofabrik.de/). 
Example for _ile-de-france_ extract below:

    mkdir -p ~/osm/extract ~/osm/planet ~/osm/output
    wget -P ~/osm/extract http://download.geofabrik.de/osm/europe/france/ile-de-france.osm.pbf

### 1.4 Plugin usage

--write-elasticsearch (--wes)

* host (default: localhost)
* port (default: 9300)
* clustername (default: elasticsearch)

### 1.5 Examples

    osmosis \
    	--read-pbf ~/osm/extract/ile-de-france.osm.pbf \
    	--way-key keyList="highway" \
    	--tag-filter reject-relations \
    	--used-node \
    	--write-elasticsearch

### 1.6 Benchmarks

* 171626 milliseconds (from pbf to pbf, 2 CPUs, 3072 Mo RAM)
* 110462 milliseconds (from pbf to osm, 2 CPUs, 3072 Mo RAM)

## 2. Administration

Some useful HTTP command:

    curl -XDELETE 'http://localhost:9200/osm/'

## 3. Resources

Osmosis plugin samples :

* Mapsforge's [mapsforge-map-writer](http://code.google.com/p/mapsforge/source/browse/trunk/mapsforge-map-writer/) plugin
* Neo4j's [neo4j-osmosis-plugin](https://github.com/svzdvd/neo4j-osmosis-plugin/) plugin
* Self-Updating Local OpenStreetMap Extract [tutorial](https://docs.google.com/document/pub?id=1paaYsOakgJEYP380R70s4SGYq8ME3ASl-mweVi1DlQ4)

ElasticSearch geospatial capabilities :

* Geo Location and Search [article](http://www.elasticsearch.org/blog/2010/08/16/geo_location_and_search.html)
* Geo Distance Filter [guide](http://www.elasticsearch.org/guide/reference/query-dsl/geo-distance-filter.html)
* Geo Point Type [guide](http://www.elasticsearch.org/guide/reference/mapping/geo-point-type.html)