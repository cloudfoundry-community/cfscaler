package main 

type Policy struct {
    Metric_type int 
    Upper_threshold float64
    Lower_threshold float64
    Instances_out int
    Instances_in int
    Cooldown_period int
    Measurement_period int
}