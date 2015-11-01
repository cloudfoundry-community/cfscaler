package main 

import (
    "encoding/json"
    "flag"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/apcera/nats"
)

var ccc CCClient
var cfg Configuration
var natsc *nats.Conn

type Configuration struct {
    CloudController map[string]string
    Nats string
    Log string
}

type SuccessMsg struct {
    App_uuid string 
    Next_time int
}

type AvgRequest struct {
    App_uuid string
    Measurement_period int
}

func init() {
    cfgPtr := flag.String("config", "config/engine.json", "Path to the config file")
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

    ccc = CCClient {
        api_host: cfg.CloudController["Api_host"],
        auth_host: cfg.CloudController["Auth_host"],
        auth_user: cfg.CloudController["Auth_user"],
        auth_pass: cfg.CloudController["Auth_pass"]}

    natsc, err = nats.Connect(cfg.Nats)
    if err != nil {
        fmt.Println("Cannot connect to the gnatsd:", err)
        os.Exit(1)
    } else {
        fmt.Println("Connected to the nats server:", cfg.Nats)
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

func Scale(msg *nats.Msg) {
    log.Printf("Received on [%s]: '%s'\n", msg.Subject, string(msg.Data))
    var app Application
    err := json.Unmarshal(msg.Data, &app)
    if err != nil {
        log.Printf("Error occurs when unmashal app object: %s", err)
        return // Skip this app
    }
    go HandleScaling(app)
}

func HandleScaling(app Application) {
    for _, policy := range app.Policies {
        start := time.Now()
        avg_metric, err := GetAvgMetric(app.App_uuid, policy.Measurement_period)
        end := time.Now()
        if err != nil {
            log.Println("Error occurs when getting avg metric:", err)
            continue // Skip this policy
        }

        var m float64
        var m_type string
        switch policy.Metric_type {
            case 0: // CPU
                m = avg_metric.Cpu
                m_type = "CPU"
            case 1: // Mem
                m = avg_metric.Mem
                m_type = "Mem"
        }

        log.Println(app.Name, m_type, "avg =", m, ", U =", policy.Upper_threshold, ", L =", policy.Lower_threshold, ", Averaging time:", end.Sub(start))

        if m > policy.Upper_threshold {
            log.Println(app.Name, "Scale out")
            num_after, err := ccc.ScaleOut(app.App_uuid, policy.Instances_out, app.Max_instances)
            if err != nil {
                log.Println(app.Name, "Scaling out failed", err)
                continue
            }
            StoreEvent(app.App_uuid, app.Name, m_type, m, policy.Upper_threshold, policy.Instances_out, num_after)
            HandleSuccess(app.App_uuid, int(time.Now().Unix()) + policy.Cooldown_period)
            return
        } else {
            if m < policy.Lower_threshold {
                log.Println(app.Name, "Scale in")
                _, err := ccc.ScaleIn(app.App_uuid, policy.Instances_in, app.Min_instances)
                if err != nil {
                    log.Println(app.Name, "Scaling in failed", err)
                    continue
                }
                HandleSuccess(app.App_uuid, int(time.Now().Unix()) + policy.Cooldown_period)
                return
            }
        }
    }
}

func GetAvgMetric(app_uuid string, measurement_period int) (Metric, error) {
    req := AvgRequest{App_uuid: app_uuid, Measurement_period: measurement_period}
    req_json, err := json.Marshal(req)
    if err != nil {
        log.Println("Error occurs when encoding avg request:", err)
        return Metric{}, err
    }

    res, err := natsc.Request("avg", req_json, 1000*time.Millisecond)
    if err != nil {
        log.Println("Error occurs when requesting avg metric:", err)
        return Metric{}, err
    }

    var avgMetric Metric
    err = json.Unmarshal(res.Data, &avgMetric)
    if err != nil {
        log.Println("Error occurs when decoding avg response:", string(res.Data))
        return Metric{}, err
    }

    return avgMetric, nil
}

func HandleSuccess(app_uuid string, next_time int) {
    msg := SuccessMsg{App_uuid: app_uuid, Next_time: next_time}
    msg_json, err := json.Marshal(msg)
    if err != nil {
        log.Println("Decode app to json failed:", err)
        return
    }
    natsc.Publish("success", msg_json)
}

/*
Store scaling event to *event* table of the CloudController database (*ccdb*)
The following is structure of the table:
id|guid|created_at|updated_at|timestamp|type|actor|actor_type|actee|actee_type|metadata|space_id|organization_guid|space_guid|actor_name|actee_name
We just care about these column:
+ created_at = time.Now()
+ type = app.autoscaling
+ actor_type: = app // there's two types: app, user
+ actee = <app_uuid>
+ actee_type = app // there's three types: space, broker, app
+ metadata = {}
+ actor_name = citusscaler
+ actee_name = <app_name>
*/
func StoreEvent(app_uuid string, app_name string, ) {

}

func main() {
    // Note: failed if name of queue group consists whitespace
    natsc.QueueSubscribe("candidates", "scale_engine", Scale)

    select {} // block forever
}