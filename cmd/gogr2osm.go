package main

import (
	"encoding/xml"
	"flag"
	"fmt"
	"log"
	"os"

	gdal "github.com/lukeroth/gdal"
	osm "github.com/paulmach/osm"
	starlark "go.starlark.net/starlark"
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

func (n *osmGenerator) TranslateLinestring(points [][2]float64) *osm.Way {
	way := n.GetNextWay()
	for _, point := range points {
		node := n.TranslatePoint(point[0], point[1])
		way.Nodes = append(way.Nodes, osm.WayNode{ID: node.ID})
	}
	n.ways = append(n.ways, way)
	return way
}

func (n *osmGenerator) TranslatePolygon(rings [][][2]float64) osm.Element {
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
		multipolygonRelation.Tags = osm.Tags{osm.Tag{Key: "type", Value: "multipolygon"}}
		multipolygonRelation.Members = append(multipolygonRelation.Members, osm.Member{
			Type: osm.TypeWay,
			Ref:  int64(ways[0].ID),
			Role: "outer",
		})

		for i := 1; i < len(ways); i++ {
			multipolygonRelation.Members = append(multipolygonRelation.Members, osm.Member{
				Type: osm.TypeWay,
				Ref:  int64(ways[i].ID),
				Role: "inner",
			})
		}
		n.relations = append(n.relations, multipolygonRelation)
		return multipolygonRelation
	} else {
		n.ways = append(n.ways, ways[0])
		return ways[0]
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

type starlarkMapper struct {
	thread         *starlark.Thread
	filterTagsFunc *starlark.Function
}

func NewStarlarkMapper(filename string) (*starlarkMapper, error) {
	thread := &starlark.Thread{Name: "osm mapper"}
	globals, err := starlark.ExecFile(thread, filename, nil, nil)
	if err != nil {
		return nil, err
	}

	filterTagsFn, found := globals["filter_tags"]
	if !found {
		log.Printf("filter_tags function not found")
	}

	return &starlarkMapper{
		thread:         thread,
		filterTagsFunc: filterTagsFn.(*starlark.Function),
	}, nil
}

func (m *starlarkMapper) FilterTags(schema gdal.FeatureDefinition, feature *gdal.Feature) (osm.Tags, error) {
	starlarkProps := starlark.NewDict(schema.FieldCount())
	for i := 0; i < schema.FieldCount(); i++ {
		fieldDefinition := schema.FieldDefinition(i)
		name := fieldDefinition.Name()
		var val starlark.Value
		switch fieldDefinition.Type() {
		case gdal.FT_Integer:
			val = starlark.MakeInt(feature.FieldAsInteger(i))
		case gdal.FT_String:
			val = starlark.String(feature.FieldAsString(i))
		case gdal.FT_Real:
			val = starlark.Float(feature.FieldAsFloat64(i))
		case gdal.FT_Integer64:
			val = starlark.MakeInt64(feature.FieldAsInteger64(i))
		default:
			return nil, fmt.Errorf("couldn't map gdal field %s (type %d)", name, fieldDefinition.Type())
		}
		starlarkProps.SetKey(starlark.String(name), val)
	}

	v, err := starlark.Call(m.thread, m.filterTagsFunc, starlark.Tuple{starlarkProps}, nil)
	if err != nil {
		if evalErr, ok := err.(*starlark.EvalError); ok {
			log.Fatal(evalErr.Backtrace())
		}
		log.Fatalf("Couldn't run filter_tag: %+v", err)
	}

	if v.Type() != "dict" {
		log.Fatalf("Expected a dict from filter_tag and got %s", v.Type())
	}

	tagsDict := v.(*starlark.Dict)
	tags := make(osm.Tags, tagsDict.Len())

	for i, t := range tagsDict.Items() {
		if t[0].Type() != "string" && t[1].Type() != "string" {
			return nil, fmt.Errorf("filter_tag returned dicts with non-string keys or values")
		}

		k := t[0].(starlark.String)
		v, ok := t[1].(starlark.String)
		if !ok {
			continue
		}

		tags[i] = osm.Tag{Key: k.GoString(), Value: v.GoString()}
	}

	return tags, nil
}

func main() {
	inputFilename := flag.String("input", "", "input path for OGR-supported file")
	outputFilename := flag.String("output", "", "output path for OSM XML data")
	mapperFilename := flag.String("mapper", "", "name of starlark file to perform mapping of input geometry to output OSM XML")
	flag.Parse()

	if *inputFilename == "" {
		log.Fatalf("No input filename specified")
	}

	source := gdal.OpenDataSource(*inputFilename, 0)
	defer source.Destroy()

	if source.LayerCount() != 1 {
		log.Fatalf("Input has more than one layer. We only support a single layer for now.")
	}

	log.Printf("Driver: %s", source.Driver().Name())

	layer := source.LayerByIndex(0)
	featureCount, ok := layer.FeatureCount(false)
	if !ok {
		log.Fatalf("Couldn't get feature count")
	}
	log.Printf("Layer name: %s; %d features", layer.Name(), featureCount)

	if *outputFilename == "" {
		log.Fatalf("No output filename specified")
	}

	if *mapperFilename == "" {
		log.Fatalf("No mapper filename specified")
	}

	mapper, err := NewStarlarkMapper(*mapperFilename)
	if err != nil {
		log.Fatalf("Couldn't create mapper: %+v", err)
	}

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

			osmPrimitive := osmGenerator.TranslatePoint(points[0][0], points[0][1])
			osmPrimitive.Tags, err = mapper.FilterTags(schema, nextFeature)
			if err != nil {
				log.Fatalf("Couldn't map tags: %+v", err)
			}

		case gdal.GT_LineString,
			gdal.GT_LineString25D,
			gdal.GT_MultiLineString,
			gdal.GT_MultiLineString25D:

			points := extractPoints(geometry)
			osmPrimitive := osmGenerator.TranslateLinestring(points)
			osmPrimitive.Tags, err = mapper.FilterTags(schema, nextFeature)
			if err != nil {
				log.Fatalf("Couldn't map tags: %+v", err)
			}

		case gdal.GT_Polygon,
			gdal.GT_Polygon25D,
			gdal.GT_MultiPolygon,
			gdal.GT_MultiPolygon25D:

			rings := extractRings(geometry)
			osmPrimitive := osmGenerator.TranslatePolygon(rings)
			switch osmPrimitive.(type) {
			case *osm.Way:
				osmPrimitive.(*osm.Way).Tags, err = mapper.FilterTags(schema, nextFeature)
			case *osm.Relation:
				osmPrimitive.(*osm.Relation).Tags, err = mapper.FilterTags(schema, nextFeature)
			}
			if err != nil {
				log.Fatalf("Couldn't map tags: %+v", err)
			}

		default:
			log.Fatalf("unknown geometry type %d", geometry.Type())
		}

		nextFeature.Destroy()
	}

	osmOutput := osmGenerator.GetOSM()
	data, err := xml.MarshalIndent(osmOutput, "", "  ")
	if err != nil {
		log.Fatalf("Couldn't marshall OSM output: %+v", err)
	}

	output, err := os.Create(*outputFilename)
	if err != nil {
		log.Fatalf("Couldn't create output file: %+v", err)
	}

	n, err := output.Write(data)
	if err != nil {
		log.Fatalf("Couldn't write to file: %+v", err)
	}

	log.Printf("Wrote %d bytes to output.osm", n)
}
