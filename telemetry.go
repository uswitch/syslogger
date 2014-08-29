package main

import (
	"github.com/amir/raidman"
	"github.com/rcrowley/go-metrics"
	"time"
	"os"
	"fmt"
	"path"
)

func metricName(name string) string {
	return fmt.Sprintf("%s %s", path.Base(os.Args[0]), name)
}

func counterEvent(name string, count int64) *raidman.Event {
	return &raidman.Event{
		Host:    "", // empty string is converted to os.Hostname() by raidman
		Service: metricName(name),
		Metric:  int(count),
        }
}

func meterEvents(name string, measure string, metric metrics.Meter) []*raidman.Event {
	return []*raidman.Event{
		&raidman.Event{
			Host: "",
			Service: metricName(fmt.Sprintf("%s-%s", name, "meanRate")),
			Metric: metric.RateMean(),
		},
		&raidman.Event{
			Host: "",
			Service: metricName(fmt.Sprintf("%s-%s", name, "oneMinuteRate")),
			Metric: metric.Rate1(),
		},
		&raidman.Event{
			Host: "",
			Service: metricName(fmt.Sprintf("%s-%s", name, "fiveMinuteRate")),
			Metric: metric.Rate5(),
		},
		&raidman.Event{
			Host: "",
			Service: metricName(fmt.Sprintf("%s-%s", name, "fifteenMinuteRate")),
			Metric: metric.Rate15(),
		},
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
			case metrics.Meter:
				events := meterEvents(name, "mean", metric.Snapshot())
				for _, e := range events {
					err := c.Send(e)
					if err != nil {
						logger.Println(err)
					}
				}
			}
		})
	}
}
