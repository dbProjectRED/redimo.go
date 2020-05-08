package redimo

type Location struct {
	Lat float64
	Lon float64
}

type Unit float64

const (
	Meters     Unit = 1.0
	Kilometers Unit = 1000.0
	Miles      Unit = 1609.34
	Feet       Unit = 0.3048
)

func (c Client) GEOADD(key string, members map[string]Location) (addedCount int64, err error) {
	return
}

func (c Client) GEODIST(key string, member1, member2 string, unit Unit) (distance float64, ok bool, err error) {
	return
}

func (c Client) GEOHASH(key string, members ...string) (geohashes []string, err error) {
	return
}

func (c Client) GEOPOS(key string, members ...string) (positions map[string]Location, err error) {
	return
}

func (c Client) GEORADIUS(key string, location Location, radius float64, radiusUnit Unit, count int64) (positions map[string]Location, err error) {
	return
}

func (c Client) GEORADIUSBYMEMBER(key string, member string, radius float64, radiusUnit Unit, count int64) (positions map[string]Location, err error) {
	return
}
