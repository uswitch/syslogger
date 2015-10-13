go-metrics-stathat
==========

A [stathat](http://www.stathat.com/) sender for [go-metrics](http://github.com/rcrowley/go-metrics)

Usage
-----

You will need to get an EZ Key from your [settings page](http://www.stathat.com/settings).

Create and update metrics on stathat:

```go
import (
	"github.com/rcrowley/go-metrics"           // to get the "metrics" namespace
	"github.com/samuraisam/go-metrics-stathat" // to get the "metricsstathat" namespace
)

// use this registry as you would normally using go-metrics
reg := metrics.NewRegistry()

// every 60 seconds, log all metrics that have been added to the provied registry to stathat
go metricsstathat.StatHat(reg, 60, "MYEZKEY")
```

Installation
------------

Uses [github.com/stathat/stathatgo](http://github.com/stathat/stathatgo) so you'll need to install it. Obviously, you'll have to be using go-metrics as well.

```sh
go get github.com/rcrowley/go-metrics // you should already have this
go get github.com/stathat/stathatgo
go get github.com/samuraisam/go-metrics-stathat
```