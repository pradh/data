package main

import (
	"context"
	"cloud.google.com/go/storage"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"googlemaps.github.io/maps"
	"io"
	"io/ioutil"
	"log"
	"os"
)

var (
	inCsvPath = flag.String("in_csv_path", "", "Input CSV file. Column names are supposed to be Place properties or "+
		"literal 'Node' to represent a local ID. containedInPlace is always assumed to be a local reference.")
	outCsvPath = flag.String("out_csv_path", "", "Same as input with additional column for DCID.")
	mapsApiKey = flag.String("maps_api_key", "", "Key for accessing Geocoding Maps API.")
)

const (
	placeId2dcidBucket = "datcom-browser-prod.appspot.com"
	placeId2dcidObject = "placeid2dcid.json"
)

type tableInfo struct {
	// Ordered rows in CSV.
	rows [][]string

	// Expanded name used for geocoding, one per row.
	extNames []string

	// Map from node-id to row index.
	node2row map[string]int

	// Various indexes. -1 if missing.
	nidIdx  int
	cipIdx  int
	nameIdx int
	typeIdx int
}

func appendContainedInPlaceNames(name string, row []string, tinfo *tableInfo) (string, error) {
	if tinfo.cipIdx < 0 {
		return name, nil
	}

	cipRef := row[tinfo.cipIdx]
	if cipRef == "" {
		return name, nil
	}

	nodeId := row[tinfo.nidIdx]
	if nodeId == cipRef {
		return name, nil
	}

	idx, ok := tinfo.node2row[cipRef]
	if !ok {
		return "", fmt.Errorf("Unresolved 'containedInPlace' ref %s in Node %s", cipRef, nodeId)
	}
	if idx >= len(tinfo.rows) {
		return "", fmt.Errorf("Out of range %d vs. %d", idx, len(tinfo.rows))
	}

	cipRow := tinfo.rows[idx]
	cipName := cipRow[tinfo.nameIdx]
	return appendContainedInPlaceNames(name+", "+cipName, cipRow, tinfo)
}

func buildTableInfo(inCsvPath string) (*tableInfo, error) {
	csvfile, err := os.Open(inCsvPath)
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(csvfile)
	isHeader := true
	rowNum := 0
	numCols := 0
	tinfo := &tableInfo{nidIdx: -1, nameIdx: -1, cipIdx: -1, typeIdx: -1}
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if isHeader {
			numCols = len(row)
			for i, f := range row {
				switch f {
				case "name":
					tinfo.nameIdx = i
				case "containedInPlace":
					tinfo.cipIdx = i
				case "typeOf":
					tinfo.typeIdx = i
				case "Node":
					tinfo.nidIdx = i
				}
			}
			if tinfo.nameIdx < 0 {
				return nil, fmt.Errorf("CSV does not have a 'name' column!")
			}
			if tinfo.cipIdx >= 0 && tinfo.nidIdx < 0 {
				return nil, fmt.Errorf("When 'containedInPLace' is provided, 'Node' must be provided to allow for references!")
			}
			isHeader = false
			rowNum += 1
			continue
		}
		if numCols != len(row) {
			return nil, fmt.Errorf("Not a rectangular CSV! Row %d has only %d columns!", rowNum, len(row))
		}
		if tinfo.nidIdx >= 0 && row[tinfo.nidIdx] != "" {
			tinfo.node2row[row[tinfo.nidIdx]] = rowNum
		}
		tinfo.rows = append(tinfo.rows, row)
		rowNum += 1
	}

	for _, row := range tinfo.rows {
		extName, err := appendContainedInPlaceNames(row[tinfo.nameIdx], row, tinfo)
		if err != nil {
			return nil, err
		}
		tinfo.extNames = append(tinfo.extNames, extName)
	}
	return tinfo, nil
}

func loadPlaceIdToDcidMap() (*map[string]string, error) {
	ctx := context.Background()
	gcsCli, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}
	fp, err := gcsCli.Bucket(placeId2dcidBucket).Object(placeId2dcidObject).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	bytes, err := ioutil.ReadAll(fp)
	if err != nil {
		return nil, err
	}
	placeId2Dcid := &map[string]string{}
	err = json.Unmarshal(bytes, &placeId2Dcid)
	if err != nil {
		return nil, err
	}
	return placeId2Dcid, nil
}

func geocodePlaces(mapsApiKey string, placeId2Dcid *map[string]string, tinfo *tableInfo) error {
	mapCli, err := maps.NewClient(maps.WithAPIKey(mapsApiKey))
	if err != nil {
		return err
	}
	for i, _ := range tinfo.rows {
		extName := tinfo.extNames[i]
		req := &maps.GeocodingRequest {
			Address:  extName,
			Language: "en",
		}
		results, err := mapCli.Geocode(context.Background(), req)
		if err != nil {
			tinfo.rows[i] = append(tinfo.rows[i], "", fmt.Sprintf("Geocoding failed for %s: %v", extName, err))
			continue
		}
		if len(results) == 0 {
			tinfo.rows[i] = append(tinfo.rows[i], "", fmt.Sprintf("Geocoding returned no result for %s", extName))
			continue
		}
		for _, result := range results {
			dcid, ok := (*placeId2Dcid)[result.PlaceID]
			if !ok {
				tinfo.rows[i] = append(tinfo.rows[i], "", fmt.Sprintf("Could not find placeId: %s", result.PlaceID))
			} else {
				// TODO: Deal with place-type checks. Deal with multiple DCIDs.
				tinfo.rows[i] = append(tinfo.rows[i], dcid, "")
			}
			break
		}
	}
	return nil
}

func writeOutput(outCsvPath string, tinfo *tableInfo) error {
	outFile, err := os.Open(outCsvPath)
	if err != nil {
		return err
	}
	w := csv.NewWriter(outFile)
	w.WriteAll(tinfo.rows)
	if err := w.Error(); err != nil {
		return err
	}
	return nil
}

func resolvePlacesByName(inCsvPath, outCsvPath, mapsApiKey string) error {
	tinfo, err := buildTableInfo(inCsvPath)
	if err != nil {
		return err
	}
	placeIdToDcid, err := loadPlaceIdToDcidMap()
	if err != nil {
		return err
	}
	err = geocodePlaces(mapsApiKey, placeIdToDcid, tinfo)
	if err != nil {
		return err
	}
	return writeOutput(outCsvPath, tinfo)
}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	err := resolvePlacesByName(*inCsvPath, *outCsvPath, *mapsApiKey)
	if err != nil {
		log.Fatalf("resolvePlacesByName failed: %v", err)
	}
}
