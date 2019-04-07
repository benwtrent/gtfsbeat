package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/benwtrent/gtfsbeat/cmd"
	"github.com/benwtrent/gtfsbeat/transit_realtime"
	"github.com/golang/protobuf/proto"

	_ "github.com/benwtrent/gtfsbeat/include"
)

func main() {
	url := "http://gtfs.viainfo.net/gtfs-realtime/trapezerealtimefeed.pb"
	client := &http.Client{}
	req, err := http.NewRequest("GET", url, nil)
	resp, err := client.Do(req)
	defer resp.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	feed := transit_realtime.FeedMessage{}
	if err := proto.Unmarshal(body, &feed); err != nil {
		log.Fatalln("Failed to parse address alerts:", err)
	}
	for _, entity := range feed.Entity {
		fmt.Println("ENTITY")
		fmt.Println(entity.Id)
		alertsJSON, _ := json.Marshal(entity.Vehicle)
		fmt.Println(string(alertsJSON))
		update, _ := json.Marshal(entity.TripUpdate)
		fmt.Println(string(update))
	}

	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
