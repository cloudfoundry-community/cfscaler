package main 

import (
    "database/sql"
    // "log"
    "time"
)

type MetricDB struct {
    db *sql.DB
}

// AvgMetric queries against MetricDB then returns average metric value of an application.
// Input: Application's UUID, measurement_period (in second)
// Output: Average metric value of the application and error
func (mdb *MetricDB) AvgMetric(app_uuid string, measurement_period int) (Metric, error) {
    metric := Metric{App_uuid: app_uuid}
    err := mdb.db.QueryRow("SELECT avg(cpu), avg(mem) FROM metrics WHERE app_uuid = ? AND created_at > ?", app_uuid, int(time.Now().Unix()) - measurement_period).Scan(&metric.Cpu, &metric.Mem)
    if err == sql.ErrNoRows {
        return Metric{}, nil
    }
    if err != nil {
        // log.Println("Error occurs when querying MetricDB: ", err)
        // log.Println("SELECT avg(cpu), avg(mem) FROM metrics WHERE app_uuid = ? AND created_at > ?", app_uuid, int(time.Now().Unix()) - measurement_period)
        return Metric{}, err
    }
    return metric, nil
}