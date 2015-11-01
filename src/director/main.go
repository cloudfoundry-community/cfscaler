package main

import (
    "database/sql"
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "os"
    "time"

    _ "github.com/go-sql-driver/mysql"
    "github.com/apcera/nats"
)

var db *sql.DB
var duration int = 10 // seconds
var cfg Configuration
var natsc *nats.Conn

type Configuration struct {
    DB map[string]string
    Duration int
    Nats string
    Log string
}

type SuccessMsg struct {
    App_uuid string 
    Next_time int
}

func Scale() {
    apps, err := GetCandidates()
    if err != nil {
        log.Println("Error occurs when scaling:", err)
        return // Skip this cycle
    }

    for _, app := range apps {
        Enqueue(app)
    }
}

func GetCandidates() ([]App, error) {
    apps := []App{}
    rows, err := db.Query("SELECT app_uuid, name, min_instances, max_instances FROM apps WHERE enabled = ? AND next_time < ?", 1, time.Now().Unix())
    if err != nil {
        log.Println("Error occurs when selecting candidates:", err)
        return apps, err
    }
    defer rows.Close()

    for rows.Next() {
        var app App
        if err := rows.Scan(&app.App_uuid, &app.Name, &app.Min_instances, &app.Max_instances); err != nil {
            log.Println("Error occurs when scanning rows:", err)
            continue // skip this app
        }

        // If there's any error than skip this app
        if err := AttachPoliciesTo(&app); err != nil {
            log.Println("Error occurs when attaching policies to app:", err)
            continue
        }

        // Just care app which have policies
        if len(app.Policies) != 0 {
            apps = append(apps, app)
        }
    }

    return apps, nil
}

// TODO: Store policies in memory and update mechanism
// MySQL Transaction per second is just thousands, we target 100 000.
// https://www.mysql.com/why-mysql/benchmarks/
func AttachPoliciesTo(app *App) error {
    policies := []Policy{}
    app.Policies = policies

    rows, err := db.Query("SELECT metric_type, upper_threshold, lower_threshold, instances_out, instances_in, cooldown_period, measurement_period FROM policies WHERE app_uuid = ? AND deleted = false", app.App_uuid)
    if err != nil {
        log.Println("Error occurs when getting policies: ", err)
        return err
    }
    defer rows.Close()

    for rows.Next() {
        var p Policy
        err := rows.Scan(&p.Metric_type, &p.Upper_threshold, &p.Lower_threshold, &p.Instances_out, &p.Instances_in, &p.Cooldown_period, &p.Measurement_period)
        if err != nil {
            log.Println("Error occurs when parsing policy: ", err)
            return err
        }
        app.Policies = append(app.Policies, p)
    }
    if err := rows.Err(); err != nil {
        log.Println("Error occurs when parsing policy: ", err)
        return err
    }
    
    return nil
}

func Enqueue(app App) {
    app_json, err := json.Marshal(app)
    if err != nil {
        log.Println("Decode app to json failed:", err)
        return // Skip this app
    }
    log.Println("Enqueue ", string(app_json))
    natsc.Publish("candidates", app_json)
}

func init() {
    cfgPtr := flag.String("config", "config/director.json", "Path to the config file")
    flag.Parse()

    f, err := os.Open(*cfgPtr)
    if err != nil {
        fmt.Println("Cannot open the config file:", err)
        os.Exit(1)
    }

    err = json.NewDecoder(f).Decode(&cfg)
    if err != nil {
        fmt.Println("Cannot decode the config file:", err)
        os.Exit(1)
    }

    db_dsn := cfg.DB["Username"]+":"+cfg.DB["Password"]+"@tcp("+cfg.DB["Host"]+":"+cfg.DB["Port"]+")/"+cfg.DB["Database"]
    db, err = sql.Open("mysql", db_dsn)
    if err != nil {
        fmt.Println("Cannot connect to the Policy database:", err)
        os.Exit(1)
    }

    if cfg.Duration != 0 {
        duration = cfg.Duration
    }

    natsc, err = nats.Connect(cfg.Nats)
    if err != nil {
        fmt.Println("Cannot connect to the gnatsd:", err)
        os.Exit(1)
    }

    if cfg.Log != "" {
        logf, err := os.Open(cfg.Log)
        if err != nil {
            fmt.Println("Cannot open the log file:", err)
            os.Exit(1)
        }
        log.SetOutput(logf)
    }
}

func HandleSuccess(msg *nats.Msg) {
    log.Printf("Received on [%s]: '%s'\n", msg.Subject, string(msg.Data))
    var success_msg SuccessMsg
    err := json.Unmarshal(msg.Data, &success_msg)
    if err != nil {
        log.Println("Error occurs when unmarshal success message: ", err)
        return // Skip this message
    }
    SetNextTime(success_msg.App_uuid, success_msg.Next_time)
}

func SetNextTime(app_uuid string, next_time int) error {
    _, err := db.Exec("UPDATE apps SET next_time = ? WHERE app_uuid = ?", next_time, app_uuid)
    if err != nil {
        log.Println("SetNextTime failed: ", err)
        return err 
    }
    return nil
}

func main() {
    defer db.Close()
    defer natsc.Close()

    natsc.Subscribe("success", HandleSuccess)

    ticker := time.NewTicker(time.Duration(duration) * time.Second)

    for t := range ticker.C {
        log.Println("Do scaling at ", t)
        go Scale()
    }
}