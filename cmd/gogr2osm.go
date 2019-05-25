package main

import (
	"encoding/xml"
	"flag"
	"fmt"
	"log"
	"os"

	gdal "github.com/lukeroth/gdal"
	osm "github.com/paulmach/osm"
)

func nodeHash(lon, lat float64) string {
	return fmt.Sprintf("%3.6f/%3.6f", lon, lat)
}

type osmGenerator struct {
	nodeCache      map[string]*osm.Node
	nodes          osm.Nodes
	ways           osm.Ways
	relations      osm.Relations
	nextNodeID     int64
	nextWayID      int64
	nextRelationID int64
}

func NewOSMGenerator() *osmGenerator {
	return &osmGenerator{
		nodeCache:      map[string]*osm.Node{},
		nodes:          osm.Nodes{},
		ways:           osm.Ways{},
		relations:      osm.Relations{},
		nextNodeID:     -1,
		nextWayID:      -1,
		nextRelationID: -1,
	}
}

func (n *osmGenerator) GetNextNode(lon, lat float64) *osm.Node {
	node := &osm.Node{
		ID:      osm.NodeID(n.nextNodeID),
		Visible: true,
		Lat:     lat,
		Lon:     lon,
	}
	n.nextNodeID--
	n.nodes = append(n.nodes, node)
	return node
}

func (n *osmGenerator) GetNextWay() *osm.Way {
	way := &osm.Way{
		ID:      osm.WayID(n.nextWayID),
		Visible: true,
	}
	n.nextWayID--
	n.ways = append(n.ways, way)
	return way
}

func (n *osmGenerator) GetNextRelation() *osm.Relation {
	relation := &osm.Relation{
		ID:      osm.RelationID(n.nextRelationID),
		Visible: true,
	}
	n.nextRelationID--
	n.relations = append(n.relations, relation)
	return relation
}

func (n *osmGenerator) TranslatePoint(lon, lat float64) *osm.Node {
	var node *osm.Node
	hash := nodeHash(lon, lat)
	if existingNode, found := n.nodeCache[hash]; found {
		node = existingNode
	} else {
		node = n.GetNextNode(lon, lat)
		n.nodeCache[hash] = node
	}

	return node
}

func (n *osmGenerator) GetOSM() *osm.OSM {
	return &osm.OSM{
		Version:   0.6,
		Nodes:     n.nodes,
		Ways:      n.ways,
		Relations: n.relations,
	}
}

func (n *osmGenerator) TranslateLinestring(points [][2]float64) {
	way := n.GetNextWay()
	for _, point := range points {
		node := n.TranslatePoint(point[0], point[1])
		way.Nodes = append(way.Nodes, osm.WayNode{ID: node.ID})
	}
	n.ways = append(n.ways, way)
}

func (n *osmGenerator) TranslatePolygon(rings [][][2]float64) {
	ways := osm.Ways{}
	for _, ring := range rings {
		way := n.GetNextWay()
		for _, point := range ring {
			node := n.TranslatePoint(point[0], point[1])
			way.Nodes = append(way.Nodes, osm.WayNode{ID: node.ID})
		}
		ways = append(ways, way)
	}

	if len(ways) > 1 {
		// If this is a polygon with holes, convert to a multipolygon relation
		multipolygonRelation := n.GetNextRelation()
		multipolygonRelation.Tags = append(multipolygonRelation.Tags, osm.Tag{Key: "type", Value: "multipolygon"})
		multipolygonRelation.Members = append(multipolygonRelation.Members, osm.Member{
			Type: ways[0].ObjectID().Type(),
			Ref:  ways[0].ObjectID().Ref(),
			Role: "outer",
		})

		for i := 1; i < len(ways); i++ {
			multipolygonRelation.Members = append(multipolygonRelation.Members, osm.Member{
				Type: ways[i].ObjectID().Type(),
				Ref:  ways[i].ObjectID().Ref(),
				Role: "inner",
			})
		}
		n.relations = append(n.relations, multipolygonRelation)
	} else {
		n.ways = append(n.ways, ways[0])
	}
}

func extractRings(geometry gdal.Geometry) [][][2]float64 {
	rings := make([][][2]float64, geometry.GeometryCount())
	for i := 0; i < geometry.GeometryCount(); i++ {
		ring := geometry.Geometry(i)
		points := extractPoints(ring)
		rings[i] = points
	}
	return rings
}

func extractPoints(geometry gdal.Geometry) [][2]float64 {
	points := make([][2]float64, geometry.PointCount())
	for i := 0; i < geometry.PointCount(); i++ {
		x, y, _ := geometry.Point(i)
		points[i] = [2]float64{x, y}
	}
	return points
}

func main() {
	flag.Parse()
	filename := flag.Arg(0)
	if filename == "" {
		log.Fatalf("No filename")
	}

	log.Printf("Found %d drivers", gdal.GetDriverCount())

	source := gdal.OpenDataSource(filename, 0)
	defer source.Destroy()

	log.Printf("Layer count: %d", source.LayerCount())
	log.Printf("Driver: %s", source.Driver().Name())

	layer := source.LayerByIndex(0)
	featureCount, ok := layer.FeatureCount(false)
	if !ok {
		log.Fatalf("Couldn't get feature count")
	}
	log.Printf("Layer name: %s; %d features", layer.Name(), featureCount)

	schema := layer.Definition()
	fieldCount := schema.FieldCount()
	for i := 0; i < fieldCount; i++ {
		fieldDefinition := schema.FieldDefinition(i)
		log.Printf("Field %2d (%s): %s", i, fieldDefinition.Type().Name(), fieldDefinition.Name())
	}

	dstSRS := gdal.CreateSpatialReference("")
	dstSRS.FromEPSG(4326)

	osmGenerator := NewOSMGenerator()

	layer.ResetReading()
	for {
		nextFeature := layer.NextFeature()
		if nextFeature == nil {
			break
		}

		geometry := nextFeature.Geometry()
		geometry.TransformTo(dstSRS)

		switch geometry.Type() {
		case gdal.GT_Point,
			gdal.GT_Point25D,
			gdal.GT_MultiPoint,
			gdal.GT_MultiPoint25D:

			points := extractPoints(geometry)

			if len(points) != 1 {
				log.Fatalf("Don't know how to handle point type with more than one point")
			}

			osmGenerator.TranslatePoint(points[0][0], points[0][1])

		case gdal.GT_LineString,
			gdal.GT_LineString25D,
			gdal.GT_MultiLineString,
			gdal.GT_MultiLineString25D:

			points := extractPoints(geometry)
			osmGenerator.TranslateLinestring(points)

		case gdal.GT_Polygon,
			gdal.GT_Polygon25D,
			gdal.GT_MultiPolygon,
			gdal.GT_MultiPolygon25D:

			rings := extractRings(geometry)
			osmGenerator.TranslatePolygon(rings)

		default:
			log.Fatalf("unknown geometry type %d", geometry.Type())
		}

		nextFeature.Destroy()
	}

	osmOutput := osmGenerator.GetOSM()
	data, err := xml.Marshal(osmOutput)
	if err != nil {
		log.Fatalf("Couldn't marshall OSM output: %+v", err)
	}

	output, err := os.Create("output.osm")
	if err != nil {
		log.Fatalf("Couldn't create output file: %+v", err)
	}

	n, err := output.Write(data)
	if err != nil {
		log.Fatalf("Couldn't write to file: %+v", err)
	}

	log.Printf("Wrote %d bytes to output.osm", n)
}
