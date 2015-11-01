package main

import (
    _ "github.com/go-sql-driver/mysql"
    "database/sql"
    "log"
) 

type Metric struct {
    Instance_uuid string
    Created_at    int
    Cpu           float64
    Mem           float64
}

type MetricDB struct {
    db *sql.DB
}

func (mdb *MetricDB) Get(app_uuid string, start int, end int, instance_uuid string) ([]Metric, error){
    var metrics []Metric

    q := "SELECT instance_uuid, cpu, mem, created_at FROM metrics WHERE app_uuid = ? AND created_at > ? AND created_at < ?"

    if instance_uuid != "" {
        q = q + " AND instance_uuid = " + instance_uuid
    }

    q = q + " ORDER BY created_at"
    
    rows, err := mdb.db.Query(q, app_uuid, start, end)
    if err != nil {
        log.Println("Error occurs when querying against metric database: ", err)
        return nil, err
    }
    defer rows.Close()

    for rows.Next() {
        var m Metric
        err := rows.Scan(&m.Instance_uuid, &m.Cpu, &m.Mem, &m.Created_at)
        if err != nil {
            log.Println("Error occurs when parsing row: ", err)
            return nil, err
        }
        metrics = append(metrics, m)
    }
    if err := rows.Err(); err != nil {
        log.Println("Error occurs when parsing rows: ", err)
        return nil, err
    }

    return metrics, nil
}

func (mdb *MetricDB) GetAvg(app_uuid string, start int, end int, step int) ([]Metric, error) {
    var metrics []Metric

    q := "SELECT avg(cpu), avg(mem) FROM metrics WHERE app_uuid = ? AND created_at > ? AND created_at < ?"

    tmp_start := start
    var tmp_metric []sql.NullFloat64
    var metric Metric 
    for tmp_start < end {
        metric.Created_at = tmp_start
        err := mdb.db.QueryRow(q, app_uuid, tmp_start, end).Scan(&tmp_metric)
        if err != nil {
            log.Println("Error occurs when querying metric database:", err)
            return metrics, err
        }
        if tmp_metric[0].Valid {
            metric.Cpu = tmp_metric[0].Float64
            metric.Mem = tmp_metric[1].Float64
        } else { // Null value
            metric.Cpu = 0
            metric.Mem = 0
        }
        metrics = append(metrics, metric)
        tmp_start = tmp_start + step
    }

    return metrics, nil
}