package beater

import (
	"encoding/csv"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/golang/protobuf/proto"

	"github.com/benwtrent/gtfsbeat/config"
	"github.com/benwtrent/gtfsbeat/transit_realtime"
)

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
	Lat  float32 `json:"lat"`
	Long float32 `json:"long"`
}

//Stop static gtfs stop definition
type Stop struct {
	ID                string
	LocationType      string
	ParentStation     string
	Code              string
	Description       string
	Name              string
	Position          GeoPoint
	Timezone          string
	URL               string
	WheelcharBoarding uint64
	ZoneID            string
}

//DenormalizedAlert An denormalized alert, indicating some sort of incident in the public transit network.
type DenormalizedAlert struct {
	Type            string       `json:"type"`
	ActivePeriod    *[]TimeRange `json:"active_period,omitempty"`
	AgencyID        *string      `json:"agency_id,omitempty"`
	RouteID         *string      `json:"route_id,omitempty"`
	RouteType       *int32       `json:"route_type,omitempty"`
	Trip            *Trip        `json:"trip,omitempty"`
	StopID          *string      `json:"stop_id,omitempty"`
	Cause           string       `json:"cause,omitempty"`
	Effect          string       `json:"effect,omitempty"`
	URL             *string      `json:"url,omitempty"`
	HeaderText      *string      `json:"header_text,omitempty"`
	DescriptionText *string      `json:"description_text,omitempty"`
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

// Gtfsbeat configuration.
type Gtfsbeat struct {
	done        chan struct{}
	config      config.Config
	client      beat.Client
	lastUpdated time.Time
	Stops       map[string]Stop
}

func addStringIfNotEmpty(key string, val string, e *beat.Event) {
	if len(val) > 0 && e != nil {
		e.PutValue(key, val)
	}
}

func addStringIfNotNull(key string, val *string, e *beat.Event) {
	if val != nil && e != nil {
		addStringIfNotEmpty(key, *val, e)
	}
}

func addUint32IfNotNull(key string, val *uint32, e *beat.Event) {
	if val != nil && e != nil {
		e.PutValue(key, *val)
	}
}

func addFloat32IfNotNull(key string, val *float32, e *beat.Event) {
	if val != nil && e != nil {
		e.PutValue(key, *val)
	}
}

func addFloat64IfNotNull(key string, val *float64, e *beat.Event) {
	if val != nil && e != nil {
		e.PutValue(key, *val)
	}
}

func addStop(stop Stop, e *beat.Event) {
	if e != nil {
		addStringIfNotEmpty("stop.id", stop.ID, e)
		addStringIfNotEmpty("stop.location_type", stop.LocationType, e)
		addStringIfNotEmpty("stop.parent_station", stop.ParentStation, e)
		addStringIfNotEmpty("stop.code", stop.Code, e)
		addStringIfNotEmpty("stop.desc", stop.Description, e)
		addStringIfNotEmpty("stop.name", stop.Name, e)
		addStringIfNotEmpty("stop.timezone", stop.Timezone, e)
		addStringIfNotEmpty("stop.url", stop.URL, e)
		if stop.Position.Lat != 0 {
			e.PutValue("stop.pos", fmt.Sprint("%f,%f", stop.Position.Lat, stop.Position.Long))
		}
		e.PutValue("stop.wheelchair_boarding", stop.WheelcharBoarding)
		addStringIfNotEmpty("stop.zone_id", stop.ZoneID, e)
		addStringIfNotEmpty("stop.id", stop.ID, e)
		addStringIfNotEmpty("stop.id", stop.ID, e)
		addStringIfNotEmpty("stop.id", stop.ID, e)
		addStringIfNotEmpty("stop.id", stop.ID, e)
		addStringIfNotEmpty("stop.id", stop.ID, e)
	}
}

func addTrip(trip *transit_realtime.TripDescriptor, e *beat.Event) {
	if trip != nil && e != nil {
		addStringIfNotNull("trip.id", trip.TripId, e)
		addStringIfNotNull("trip.route_id", trip.RouteId, e)
		addUint32IfNotNull("trip.direction_id", trip.DirectionId, e)
		e.PutValue("trip.state", trip.GetScheduleRelationship().String())
		addStringIfNotNull("trip.id", trip.TripId, e)
		if trip.StartTime != nil {
			date := time.Now()
			if trip.StartDate != nil {
				var err error
				date, err = time.Parse("20060102", *trip.StartDate)
				if err != nil {
					logp.Error(err)
				} else {
					date = time.Now()
				}
			}
			year, month, day := date.Date()
			startTime, err := time.Parse("20060102 15:04:05", string(year)+string(month)+string(day)+" "+*trip.StartTime)
			if err != nil {
				logp.Error(err)
			} else {
				e.PutValue("trip.start_time", startTime)
			}
		}
	}
}

func addVehicleDescriptors(vehicleDescriptors *transit_realtime.VehicleDescriptor, e *beat.Event) {
	if vehicleDescriptors != nil && e != nil {
		addStringIfNotNull("vehicle.id", vehicleDescriptors.Id, e)
		addStringIfNotNull("vehicle.label", vehicleDescriptors.Label, e)
		addStringIfNotNull("vehicle.license_plate", vehicleDescriptors.LicensePlate, e)
	}
}

//DenormalizeAlert denormalizes a gtfs alert
func DenormalizeAlert(alert *transit_realtime.Alert) []beat.Event {
	events := make([]beat.Event, len(alert.InformedEntity))
	timeRange := make([]TimeRange, len(alert.ActivePeriod))
	for i, t := range alert.ActivePeriod {
		timeRange[i] = TimeRange{t.Start, t.End}
	}

	for i, entity := range alert.GetInformedEntity() {
		event := beat.Event{}
		id := ""
		if entity.AgencyId != nil {
			id += *entity.AgencyId
		}
		if entity.RouteId != nil {
			id += *entity.RouteId
		}
		if entity.StopId != nil {
			id += *entity.StopId
		}
		if alert.DescriptionText != nil {
			h := fnv.New32a()
			h.Write([]byte(*(*alert.DescriptionText).GetTranslation()[0].Text))
			id += string(h.Sum32())
		}
		event.SetID(id)
		event.PutValue("alert_cause", alert.GetCause().String())
		event.PutValue("alert_effect", alert.GetEffect().String())
		if len(timeRange) > 0 {
			event.PutValue("active_period", timeRange)
		}
		if alert.Url != nil {
			event.PutValue("url", alert.Url.GetTranslation()[0])
		}
		if alert.DescriptionText != nil {
			event.PutValue("description", alert.DescriptionText.GetTranslation()[0])
		}
		if alert.HeaderText != nil {
			event.PutValue("header", alert.HeaderText.GetTranslation()[0])
		}
		addStringIfNotNull("agency_id", entity.AgencyId, &event)
		addStringIfNotNull("route_id", entity.AgencyId, &event)
		addStringIfNotNull("route_type", entity.AgencyId, &event)
		addStringIfNotNull("stop", entity.AgencyId, &event)
		addTrip(entity.Trip, &event)
		events[i] = event
	}
	return events
}

//TransformVehicle transforms a gtfs vehicle position
func (bt *Gtfsbeat) TransformVehicle(vehicle *transit_realtime.VehiclePosition) beat.Event {
	event := beat.Event{
		Fields: common.MapStr{},
	}
	event.PutValue("congestion", vehicle.GetCongestionLevel().String())
	event.PutValue("occupancy", vehicle.GetOccupancyStatus().String())
	event.PutValue("stop_status", vehicle.GetCurrentStatus().String())
	addTrip(vehicle.Trip, &event)
	addVehicleDescriptors(vehicle.Vehicle, &event)
	if vehicle.Position != nil {
		if vehicle.Position.Latitude != nil && vehicle.Position.Longitude != nil {
			event.PutValue("pos", fmt.Sprintf("%f,%f", *vehicle.Position.Latitude, *vehicle.Position.Longitude))
		}
		addFloat32IfNotNull("bearing", vehicle.Position.Bearing, &event)
		addFloat64IfNotNull("odometer_meters", vehicle.Position.Odometer, &event)
		addFloat32IfNotNull("speed_meters_per_sec", vehicle.Position.Speed, &event)
		if vehicle.Position.Speed != nil {
			event.PutValue("speed_mph", (*vehicle.Position.Speed)*2.2369362921)
		}
		addFloat32IfNotNull("speed_meters_per_sec", vehicle.Position.Speed, &event)
	}
	addUint32IfNotNull("stop_seq", vehicle.CurrentStopSequence, &event)
	if vehicle.Timestamp != nil {
		event.Timestamp = time.Unix(int64(*vehicle.Timestamp), 0)
	}
	if vehicle.StopId != nil {
		logp.Info("stopid %s", *vehicle.StopId)
		logp.Info("seq %d", *vehicle.CurrentStopSequence)
		event.PutValue("stop.id", *vehicle.StopId)
		if stop, ok := bt.Stops[*vehicle.StopId]; ok {
			if ok {
				addStop(stop, &event)
			} else {
				logp.Warn("Unrecognized stop id %s", *vehicle.StopId)
			}
		}
	}
	event.PutValue("stop_status", vehicle.GetCurrentStatus().String())
	return event
}

//DenormalizeTripUpdate denormalizes a gtfs trip update
func DenormalizeTripUpdate(tripupdate *transit_realtime.TripUpdate) beat.Event {
	event := beat.Event{}
	addTrip(tripupdate.Trip, &event)
	addVehicleDescriptors(tripupdate.Vehicle, &event)
	// TODO fix....
	return event
}

func parseStops(fileName string) (map[string]Stop, error) {
	f, err := os.Open(fileName)
	if err != nil {
		logp.Error(err)
		return nil, err
	}
	defer f.Close()
	csvr := csv.NewReader(f)
	stops := map[string]Stop{}
	for {
		row, err := csvr.Read()
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			return stops, err
		}
		// Skip the header
		if row[0] == "stop_id" {
			continue
		}
		stop := Stop{
			ID:            row[0],
			Code:          row[1],
			Name:          row[2],
			Description:   row[3],
			ZoneID:        row[6],
			URL:           row[7],
			LocationType:  row[8],
			ParentStation: row[9],
			Timezone:      row[10],
		}
		if stop.WheelcharBoarding, err = strconv.ParseUint(row[11], 10, 64); err != nil {
			return nil, err
		}
		var lat float64
		var lon float64
		if lat, err = strconv.ParseFloat(row[4], 64); err != nil {
			return nil, err
		}
		if lon, err = strconv.ParseFloat(row[5], 64); err != nil {
			return nil, err
		}
		stop.Position = GeoPoint{
			Lat:  float32(lat),
			Long: float32(lon),
		}
		stops[row[0]] = stop
	}
}

// New creates an instance of gtfsbeat.
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}
	bt := &Gtfsbeat{
		done:        make(chan struct{}),
		config:      c,
		lastUpdated: time.Now().UTC(),
	}
	var err error
	bt.Stops, err = parseStops(c.Stops)
	if err != nil {
		logp.Error(err)
		return nil, err
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
	if resp.Header.Get("Last-Modified") != "" {
		if lastModified, err := http.ParseTime(resp.Header.Get("Last-Modified")); err != nil {
			if lastModified.Before(bt.lastUpdated) {
				logp.Info("Data has not been updated since %s. Last update %s", lastModified, bt.lastUpdated)
				return nil, nil
			}
			bt.lastUpdated = lastModified
		}
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
		events := []beat.Event{}
		if err != nil {
			logp.Error(err)
		} else if feedentity != nil {
			for _, entity := range feedentity {
				if entity.Vehicle != nil {
					events = append(events, bt.TransformVehicle(entity.Vehicle))
				}
			}
		}
		if len(events) > 0 {
			bt.client.PublishAll(events)
		}
		logp.Info("Events sent: %d", len(events))
		counter++
	}
}

// Stop stops gtfsbeat.
func (bt *Gtfsbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}
