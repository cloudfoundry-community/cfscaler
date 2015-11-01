package main

type Metadata struct {
    Scale string // scale type: in, out
    Metric string // metric type
    Value float64 // current value of the metric
    Threshold float64
    Status int // 1 - Success, 0 - Failed
    InstancesOut int // number of instances be scaled
    NumAfter int // number of running instances after scaling
    CreatedAt int // Unix timestamp
}