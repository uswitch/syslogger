package main

import (
	"github.com/amir/raidman"
	"github.com/rcrowley/go-metrics"
	"time"
)

func counterEvent(name string, count int64) *raidman.Event {
	return &raidman.Event{
		Host:    "", // empty string is converted to os.Hostname() by raidman
		Service: name,
		Metric:  int(count),
        }
}

func Raybans(r metrics.Registry, d time.Duration, c *raidman.Client) {
	for _ = range time.Tick(d) {
		r.Each(func(name string, i interface{}) {
			switch metric := i.(type) {
			case metrics.Counter:
				e := counterEvent(name, metric.Count())
				err := c.Send(e)
				if err != nil {
					logger.Println(err)
				}
			}
		})
	}
}
