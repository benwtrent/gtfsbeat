// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type Config struct {
	Period         time.Duration `config:"period"`
	URL            string        `config:"url"`
	Agency         string        `config:"agency"`
	Stops          string        `config:"stops"`
	Routes         string        `config:"routes"`
	Trips          string        `config:"trips"`
	StopTimes      string        `config:"stop_times"`
	Calendar       string        `config:"calendar"`
	CalendarDates  string        `config:"calendar_dates"`
	FareAttributes string        `config:"fare_attributes"`
	FareRules      string        `config:"fare_rules"`
	Shapes         string        `config:"Shapes"`
	Frequency      string        `config:"frequency"`
	Transfers      string        `config:"transfers"`
	FeedInfo       string        `config:"feed_info"`
}

var DefaultConfig = Config{
	Period:         5 * time.Minute,
	URL:            "http://gtfs.viainfo.net/gtfs-realtime/trapezerealtimefeed.pb",
	Agency:         "./agency.txt",
	Stops:          "./stops.txt",
	Routes:         "./routes.txt",
	Trips:          "./trips.txt",
	StopTimes:      "./stop_times.txt",
	Calendar:       "./calendar.txt",
	CalendarDates:  "./calendar_dates.txt",
	FareAttributes: "./fare_attributes.txt",
	FareRules:      "./fare_rules.txt",
	Shapes:         "./shapes.txt",
	Frequency:      "./frequency.txt",
	Transfers:      "./transfers.txt",
	FeedInfo:       "./feed_info.txt",
}
