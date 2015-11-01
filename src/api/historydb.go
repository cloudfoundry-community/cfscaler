package main

import (
    "database/sql"
    "encoding/json"
    "log"

    _ "github.com/lib/pq"
) 

type HistoryDB struct {
    db *sql.DB
}

func (hdb *HistoryDB) Get(app_uuid string, start int, end int) ([]Metadata, error){
    var result []Metadata
    q := "SELECT metadata FROM event WHERE actor_name = $1 AND actee = $2 AND created_at > $3 AND created_at < $4 ORDER BY created_at"
    
    rows, err := hdb.db.Query(q, "citusscaler", app_uuid, start, end)
    if err != nil {
        log.Println("Error occurs when querying against history database: ", err)
        return nil, err
    }
    defer rows.Close()

    for rows.Next() {
        var metadata string
        err := rows.Scan(&metadata)
        if err != nil {
            log.Println("Error occurs when parsing row: ", err)
            return nil, err
        }

        var m Metadata
        err1 := json.Unmarshal([]byte(metadata), m)
        if err1 != nil {
            log.Println("Error occuers when decoding metadata:", err1)
            return nil, err1
        }
        
        result = append(result, m)
    }

    if err := rows.Err(); err != nil {
        log.Println("Error occurs when parsing rows: ", err)
        return nil, err
    }

    return result, nil
}