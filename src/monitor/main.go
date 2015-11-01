package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
)

type Metric struct {
	Cpu float64 `json:"cpu"`
	Mem float64 `json:"mem"`
}

type MetricRow struct {
	App_uuid      string
	Instance_uuid string
	Created_at    int
	Cpu           float64
	Mem           float64
}

type Configuration struct {
	Port     string
	Database string
	User     string
	Password string
	AvgerHost string
	AvgerPort string
}

func handleConnection(avgerConn net.Conn, c net.Conn, db *sql.DB) {
	scanner := bufio.NewScanner(c)
	for scanner.Scan() {
		handleLine(avgerConn, scanner.Text(), db)
	}
	if err := scanner.Err(); err != nil {
		log.Println("Cannot read the connection input:", err)
	}
}

func handleLine(avgerConn net.Conn, line string, db *sql.DB) {
	// As TSDB protocol, line has format:
	// "put key timestamp value tags\n"
	// tags: "#{key}=#{v}"
	// More details at CF Collector > historian/tsdb.rb
	elements := strings.Split(line, " ")

	if elements[1] == "app_metrics" {
		var apps map[string](map[string]Metric)
		err := json.Unmarshal([]byte(elements[3]), &apps)
		if err != nil {
			log.Fatal("Cannot decode the JSON message:", err)
		}

		for app_uuid, instances := range apps {
			for instance_uuid, metric := range instances {
				_, err := db.Exec("INSERT INTO metrics (app_uuid, instance_uuid, created_at, cpu, mem) VALUES (?, ?, ?, ?, ?);", app_uuid, instance_uuid, int32(time.Now().Unix()), metric.Cpu, metric.Mem)
				if err != nil {
					log.Println("Cannot insert to the database:", err)
				}
				log.Println("Saved: ", app_uuid, instance_uuid, "cpu: ", metric.Cpu, "mem: ", metric.Mem)

				message := app_uuid + " " + instance_uuid + " " + strconv.FormatFloat(metric.Cpu, 'f', -1, 64) + " " + strconv.FormatFloat(metric.Mem, 'f', -1, 64) + "\n"
				log.Println(message)
				avgerConn.Write([]byte(message))
			}
		}
	}
}

func main() {
	cfgPtr := flag.String("config", "config/monitor.json", "Path to the config file")
	flag.Parse()

	f, err := os.Open(*cfgPtr)
	if err != nil {
		log.Fatal("Cannot open the config file:", err)
	}

	var cfg Configuration
	decoder := json.NewDecoder(f)
	err = decoder.Decode(&cfg)
	if err != nil {
		log.Fatal("Cannot decode the config file: ", err)
	}

	db, err := sql.Open("mysql", cfg.User+":"+cfg.Password+"@/"+cfg.Database)
	if err != nil {
		log.Fatal("Cannot connect to the database:", err)
	}
	defer db.Close()

	// Listen on TCP port 4567 on all interfaces.
	l, err := net.Listen("tcp", ":"+cfg.Port)
	if err != nil {
		log.Fatal("Cannot listen on the port:", cfg.Port, err)
	}
	defer l.Close()
	fmt.Println("Listening on 0.0.0.0, port", cfg.Port)

	avgerConn, err := net.Dial("tcp", cfg.AvgerHost+":"+cfg.AvgerPort)
	if err != nil {
		log.Fatal("Cannot connect to the avger: ", cfg.AvgerHost, ":", cfg.AvgerPort)
	}
	defer avgerConn.Close()

	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal("Cannot accept the connection", err)
		}

		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go handleConnection(avgerConn, conn, db)
	}
}
