package main 

import (
    "bufio"
    "database/sql"
    "flag"
    "log"
    "net"
    "fmt"
    "encoding/json"
    "os"
    "strconv"
    "time"

    "github.com/apcera/nats"
    _ "github.com/go-sql-driver/mysql"
)

var cfg Configuration
var natsc *nats.Conn
var mdb MetricDB
var mdb_conn *sql.DB

type Configuration struct {
    MetricDB map[string]string
    MonitorHost string
    MonitorPort string
    Nats string
}

type Metric struct {
    App_uuid string
    Cpu float64
    Mem float64 
}

type AvgRequest struct {
    App_uuid string
    Measurement_period int
}

func handleMonitor(conn net.Conn) {
    scanner := bufio.NewScanner(conn)
    for scanner.Scan() {
        elements := strings.Split(scanner.Text(), " ")

        var m Metric
        m.App_uuid = elements[0]
        m.Cpu = strconv.ParseFloat(elements[2], 64)
        m.Mem = strconv.ParseFloat(elements[3], 64)

        avger.AddMetric(m)
    }

    if err := scanner.Err(); err != nil {
        log.Println("Cannot read the input from monitor:", err)
    }
}

func HandleEngine(msg *nats.Msg) {
    log.Printf("Received on [%s]: '%s'\n", msg.Subject, string(msg.Data))

    var req AvgRequest
    err := json.Unmarshal(msg.Data, &req)
    if err != nil {
        log.Println("Error occurs when decoding avg response", err)
        return
    }

    start := time.Now()
    avgMetric := GetAvgMetric(req.App_uuid, req.Measurement_period)
    end := time.Now()
    log.Println("Averaging time", end.Sub(start))
    avgMetric_json, err := json.Marshal(avgMetric)
    if err != nil {
        log.Println("Error occurs when encoding avg request:", err)
        return
    }

    natsc.Publish(msg.Reply, avgMetric_json)
}

func GetAvgMetric(app_uuid string, measurement_period int) Metric {
    // metric, err := mdb.AvgMetric(app_uuid, measurement_period)

    var r AvgRequest
    r.App_uuid = app_uuid
    r.Measurement_period = measurement_period

    metric, err := avger.GetAvgMetric(r)
    if err != nil {
        log.Println("Error occurs when getting avg metric:", err)
        return Metric{}
    }

    return metric
}

func init() {
    cfgPtr := flag.String("config", "config/avger.json", "Path to the config file")
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

    natsc, err = nats.Connect(cfg.Nats)
    if err != nil {
        fmt.Println("Cannot connect to the gnatsd:", err)
        os.Exit(1)
    }

    var avger Avger

    // // MetricDB connection
    // mdb_dsn :=  cfg.MetricDB["Username"]+":"+
    //             cfg.MetricDB["Password"]+"@tcp("+
    //             cfg.MetricDB["Host"]+":"+
    //             cfg.MetricDB["Port"]+")/"+
    //             cfg.MetricDB["Database"]
    // mdb_conn, err = sql.Open("mysql", mdb_dsn)
    // if err != nil {
    //     fmt.Println("Cannot connect to the Policy database:", err)
    //     os.Exit(1)
    // }
    // mdb = MetricDB {db: mdb_conn}
}

func main() {
    l, err := net.Listen("tcp", cfg.MonitorHost + ":" + cfg.MonitorPort)
    if err != nil {
        log.Fatal("Cannot listen to: ", cfg.MonitorHost, cfg.MonitorPort, err)
    }
    defer l.Close()
    log.Println("Listening to monitor connection on", cfg.MonitorHost, cfg.MonitorPort)

    natsc.Subscribe("avg", HandleEngine)

    for {
        conn, err := l.Accept()
        if err != nil {
            log.Println("Accept connection from monitor failed: ", err)
        } else {
            log.Println("Accept connection from monitor: ", conn.RemoteAddr().String())
            go handleMonitor(conn)
        }
    }
}