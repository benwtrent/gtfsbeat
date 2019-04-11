// Config is put into a different package to prevent cyclic imports in case
// it is needed in several locations

package config

import "time"

type Config struct {
	Period time.Duration `config:"period"`
	URL    string        `config:"url"`
}

var DefaultConfig = Config{
	Period: 1 * time.Second,
	URL:    "http://gtfs.viainfo.net/gtfs-realtime/trapezerealtimefeed.pb",
}
