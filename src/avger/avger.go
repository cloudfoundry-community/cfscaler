package main

import (
    "time"
)

const MAX_MEASUREMENT_PERIOD = 3600 // seconds

type Avger struct {
    Apps map[string]App
}

type App struct {
    RawMetrics []RawMetric

    // TODO: Pre-computed values
    Avg1m float64 // avg of 1 most recent minute
    Avg5m float64 // avg of 5 most recent minutes
    Avg10m float64 // avg of 10 most recent minutes
    Avg30m float64 // avg of 30 most recent minites
}

type RawMetric struct {
    Time int
    Cpu float64
    Mem float64
}

func (avger *Avger) AddMetric(m Metric) {
    app, exist := avger.Apps[m.App_uuid]
    if exist == false {
        avger.Apps[m.App_uuid] = app
    }

    avger.Apps[m.App_uuid].AddMetric(m)
}

func (avger *Avger) GetAvgMetric(r AvgRequest) Metric {
    _, exist := avger.Apps[r.App_uuid]
    if exist == false {
        return Metric{}
    }

    return avger.Apps[r.App_uuid].GetAvgMetric(r)
}

func (app *App) AddMetric(m Metric) {
    var rm RawMetric
    rm.Time = time.Now().Unix()
    rm.Cpu = m.Cpu
    rm.Mem = m.Mem

    app.Clean()
    append(app.RawMetrics, rm)
}

func (app *App) GetAvgMetric(r AvgRequest) Metric {
    var m Metric
    m.App_uuid = r.App_uuid

    var counter int
    var sum_cpu float64
    var sum_mem float64
    t_now := time.Now().Unix()
    
    for i := len(app.RawMetrics); i > 0; i-- {
        if app.RawMetrics[i].Time + r.Measurement_period < t_now {
            break
        }
        counter := counter + 1
        sum_cpu := sum_cpu + app.RawMetrics[i].Cpu
        sum_mem := sum_mem + app.RawMetrics[i].Mem
    }

    if counter == 0 {
        return Metric{}
    }

    m.Cpu = sum_cpu / counter
    m.Mem = sum_mem / counter

    return m
}

func (app *App) Clean() {
    for i, rm := range app.RawMetrics {
        if rm.Time > time.Now().Unix() - MAX_MEASUREMENT_PERIOD {
            app.RawMetrics = app.RawMetrics[i:]
            return
        }
    }
}