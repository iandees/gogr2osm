package main

import (
	"flag"
	"log"

	gdal "github.com/lukeroth/gdal"
)

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

	layer.ResetReading()
	for {
		nextFeature := layer.NextFeature()
		if nextFeature == nil {
			break
		}

		geometry := nextFeature.Geometry()
		geometry.TransformTo(dstSRS)

		switch geometry.Type() {
		case gdal.GT_Point:
			x, y, _ := geometry.Point(0)
			log.Printf("point with lat/lng: %0.6f,%0.6f", y, x)
		case gdal.GT_LineString:
			log.Printf("linestring with %d geometry", geometry.GeometryCount())
		case gdal.GT_Polygon:
			log.Printf("polygon with %d rings", geometry.GeometryCount())
		}

		nextFeature.Destroy()
	}
}
