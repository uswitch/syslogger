package main

import (
	"github.com/amir/raidman"
	"github.com/rcrowley/go-metrics"
	"github.com/cenkalti/backoff"
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

func meterEvents(name string, metric metrics.Meter) []*raidman.Event {
	return []*raidman.Event{
		event(name, "meanRate", metric.RateMean()),
		event(name, "oneMinuteRate", metric.Rate1()),
		event(name, "fiveMinuteRate", metric.Rate5()),
		event(name, "fifteenMinuteRate", metric.Rate15()),
	}
}

func event(name string, measure string, val interface{}) *raidman.Event {
	return &raidman.Event{
		Host: "",
		Service: metricName(fmt.Sprintf("%s-%s", name, measure)),
		Metric: val,
	}
}

func histogramEvents(name string, metric metrics.Histogram) []*raidman.Event {
	events := []*raidman.Event{
		event(name, "min", int(metric.Min())),
		event(name, "max", int(metric.Max())),
		event(name, "mean", metric.Mean()),
		event(name, "std-dev", metric.StdDev()),
	}

	percentiles := []float64 {0.75, 0.95, 0.99, 0.999}
	percentileVals := metric.Percentiles(percentiles)
	for i, p := range percentiles {
		e := event(name, fmt.Sprintf("percentile %.3f", p), percentileVals[i])
		events = append(events, e)
	}
	return events
}

type InfiniteBackoff struct {
	RetryInterval time.Duration
}
func (b *InfiniteBackoff) NextBackOff() time.Duration {
	return b.RetryInterval
}
func (b *InfiniteBackoff) Reset() {
}

func establishRiemannClient() chan *raidman.Client {
	connChannel := make(chan *raidman.Client)

	go func() {
		connect := func() error {
			c, err := raidman.Dial("tcp", cfg.riemann)
			if err != nil {
				logger.Println("Error connecting to Riemann, will retry.", err)
				return err
			} else {
				connChannel <- c
				return nil
			}
		}

		policy := &InfiniteBackoff{time.Second * 5}
		backoff.Retry(connect, policy)
	}()

	return connChannel
}

func Raybans(r metrics.Registry, d time.Duration) {
	var c *raidman.Client
	ch := establishRiemannClient()
	c = <- ch

	for _ = range time.Tick(d) {
		r.Each(func(name string, i interface{}) {
			switch metric := i.(type) {
			case metrics.Counter:
				e := counterEvent(name, metric.Count())

				err := c.Send(e)
				if err != nil {
					logger.Println("error sending riemann metric.", err)
					c.Close()
					ch := establishRiemannClient()
					c = <- ch
				}
			case metrics.Meter:
				events := meterEvents(name, metric.Snapshot())
				for _, e := range events {
					err := c.Send(e)
					if err != nil {
						logger.Println("error sending riemann metric.", err)
						c.Close()
						ch := establishRiemannClient()
						c = <- ch
					}
				}
			case metrics.Histogram:
				events := histogramEvents(name, metric.Snapshot())
				for _, e := range events {
					err := c.Send(e)
					if err != nil {
						logger.Println("error sending riemann metric.", err)
						c.Close()
						ch := establishRiemannClient()
						c = <- ch
					}
				}
			}
		})
	}
}
