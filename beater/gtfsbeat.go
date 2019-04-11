package beater

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/golang/protobuf/proto"

	"github.com/benwtrent/gtfsbeat/config"
	"github.com/benwtrent/gtfsbeat/transit_realtime"
)

// Gtfsbeat configuration.
type Gtfsbeat struct {
	done   chan struct{}
	config config.Config
	client beat.Client
}

//TimeRange simple timerange
type TimeRange struct {
	Start *uint64 `json:"gte,omitempty"`
	End   *uint64 `json:"lte,omitempty"`
}

//Trip collection of trip information
type Trip struct {
	ID                   *string `json:"trip_id,omitempty"`
	RouteID              *string `json:"route_id,omitempty"`
	DirectionID          *uint32 `protobuf:"varint,6,opt,name=direction_id,json=directionId" json:"direction_id,omitempty"`
	StartTime            *uint64 `json:"start_time,omitempty"`
	ScheduleRelationship *string `json:"schedule_relationship,omitempty"`
}

//GeoPoint simple geopoint type
type GeoPoint struct {
	Lat  *float32 `json:"lat,omitempty"`
	Long *float32 `json:"long,omitempty"`
}

//DenormalizedAlert An denormalized alert, indicating some sort of incident in the public transit network.
type DenormalizedAlert struct {
	Type            string     `json:"type"`
	ActivePeriod    *TimeRange `json:"active_period,omitempty"`
	AgencyID        *string    `json:"agency_id,omitempty"`
	RouteID         *string    `json:"route_id,omitempty"`
	RouteType       *int32     `json:"route_type,omitempty"`
	Trip            *Trip      `json:"trip,omitempty"`
	StopID          *string    `json:"stop_id,omitempty"`
	Cause           string     `json:"cause,omitempty"`
	Effect          string     `json:"effect,omitempty"`
	URL             *string    `json:"url,omitempty"`
	HeaderText      *string    `json:"header_text,omitempty"`
	DescriptionText *string    `json:"description_text,omitempty"`
}

//Vehicle identification information
type Vehicle struct {
	ID           *string `json:"id,omitempty"`
	Label        *string `json:"label,omitempty"`
	LicensePlate *string `json:"license_plate,omitempty"`
}

//VehicleStatus data
type VehicleStatus struct {
	Type                string    `json:"type"`
	Trip                *Trip     `json:"trip,omitempty"`
	Vehicle             *Vehicle  `json:"vehicle,omitempty"`
	Position            *GeoPoint `json:"position,omitempty"`
	Bearing             *float32  `json:"bearing,omitempty"`
	Odometer            *float64  `json:"odometer_meters,omitempty"`
	Speed               *float32  `json:"speed_meters,omitempty"`
	CurrentStopSequence *uint32   `json:"stop_seq,omitempty"`
	StopID              *string   `json:"stop_id,omitempty"`
	StopStatus          *string   `json:"stop_status,omitempty"`
	CongestionLevel     *string   `json:"congestion,omitempty"`
	OccupancyStatus     *string   `json:"occupancy,omitempty"`
}

//StopEvent collection of stop event information
type StopEvent struct {
	Delay       *int32 `json:"delay,omitempty"`
	Time        *int64 `json:"time,omitempty"`
	Uncertainty *int32 `json:"uncertainty,omitempty"`
}

//TripUpdate denormalized trip update
type TripUpdate struct {
	Type                 string     `json:"type"`
	Trip                 *Trip      `json:"trip,omitempty"`
	Vehicle              *Vehicle   `json:"vehicle,omitempty"`
	Delay                *int32     `json:"delay,omitempty"`
	StopSequence         *uint32    `json:"stop_seq,omitempty"`
	StopID               *string    `json:"stop_id,omitempty"`
	Arrival              *StopEvent `json:"arrival,omitempty"`
	Departure            *StopEvent `json:"departure,omitempty"`
	ScheduleRelationship *string    `json:"stop_relationship,omitempty"`
}

//DenormalizeAlert denormalizes a gtfs alert
func DenormalizeAlert(alert *transit_realtime.Alert) []DenormalizedAlert {
	return []DenormalizedAlert{}
}

//TransformVehicle transforms a gtfs vehicle position
func TransformVehicle(vehicle *transit_realtime.VehiclePosition) VehicleStatus {
	return VehicleStatus{}
}

//DenormalizeTripUpdate denormalizes a gtfs trip update
func DenormalizeTripUpdate(tripupdate *transit_realtime.TripUpdate) []TripUpdate {
	return []TripUpdate{}
}

// New creates an instance of gtfsbeat.
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Gtfsbeat{
		done:   make(chan struct{}),
		config: c,
	}
	return bt, nil
}

//GetGtfsFeed gathers the feed entity
func (bt *Gtfsbeat) GetGtfsFeed() ([]*transit_realtime.FeedEntity, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", bt.config.URL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	logp.Debug("Received gtfs feed: %s", resp.Status)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	feed := transit_realtime.FeedMessage{}
	if resp.StatusCode != 200 {
		sbody := string(body)
		logp.Warn("Received gtfs realtime response but with errors: %s", sbody)
		return feed.Entity, errors.New(sbody)
	}
	if err := proto.Unmarshal(body, &feed); err != nil {
		logp.Error(err)
		return nil, err
	}
	return feed.GetEntity(), nil
}

// Run starts gtfsbeat.
func (bt *Gtfsbeat) Run(b *beat.Beat) error {
	logp.Info("gtfsbeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(bt.config.Period)
	counter := 1
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}
		feedentity, err := bt.GetGtfsFeed()
		if err != nil {
			logp.Error(err)
		} else {
			for _, entity := range feedentity {
				fmt.Println("ENTITY")
				if entity.Id != nil {
					fmt.Printf("Entity ID: %v \n", *entity.Id)
				} else {
					fmt.Println("<nil>")
				}
				if entity.Vehicle != nil {
					fmt.Printf("Vehicle: %+v\n", *entity.Vehicle)
				}
				if entity.Alert != nil {
					fmt.Printf("Alert: %+v\n", *entity.Alert)
				}
				if entity.TripUpdate != nil {
					fmt.Printf("TripUpdate: %+v\n", *entity.TripUpdate)
				}
			}
		}
		event := beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    b.Info.Name,
				"counter": counter,
			},
		}
		bt.client.Publish(event)
		logp.Info("Event sent")
		counter++
	}
}

// Stop stops gtfsbeat.
func (bt *Gtfsbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}
