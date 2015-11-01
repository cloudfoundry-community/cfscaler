package main 

import(
    "fmt"
    "os"
    "flag"
    "encoding/json"
    "database/sql"
    "net/http"
    "log"
    "strconv"
    "time"

    "github.com/gorilla/mux"
    // tuna
    "github.com/robfig/cron"
    // end tuna
)

const (
    ErrInvalidParam = `{"error": "Invalid parameters"}`
    ErrServerFailed = `{"error": "Server failed"}`
    ErrMisssingParam = `{"error": "Missing parameter"}`
    ErrExisting = `{"error": "Existing"}`
    ErrNotExist = `{"error": "Not exist"}`
    SuccessMsg = `{"message": "Successfully"}`
)

type API struct {
    mdb *MetricDB 
    pdb *PolicyDB
    hdb *HistoryDB
}

type Configuration struct {
    PolicyDB map[string]string
    MetricDB map[string]string
    HistoryDB map[string]string
    Logfile string
    Port string
}

var api API
var cfg Configuration

func init() {
    fmt.Println("API server running...")
    configPtr := flag.String("config", "config/api.json", "Path to the config file")
    flag.Parse()

    f, err := os.Open(*configPtr)
    if err != nil {
        fmt.Println("Cannot open the config file: ", err)
        os.Exit(1)
    }

    err = json.NewDecoder(f).Decode(&cfg)
    if err != nil {
        fmt.Println("Cannot decode the config file: ", err)
        os.Exit(1)
    }

    // PolicyDB connection
    pdb_dsn :=  cfg.PolicyDB["Username"]+":"+
                cfg.PolicyDB["Password"]+"@tcp("+
                cfg.PolicyDB["Host"]+":"+
                cfg.PolicyDB["Port"]+")/"+
                cfg.PolicyDB["Database"]
    pdb_conn, err := sql.Open("mysql", pdb_dsn)
    if err != nil {
        log.Fatal("Cannot connect to the Policy database:", err)
    }
    // defer pdb_conn.Close()

    // MetricDB connection
    mdb_dsn :=  cfg.MetricDB["Username"]+":"+
                cfg.MetricDB["Password"]+"@tcp("+
                cfg.MetricDB["Host"]+":"+
                cfg.MetricDB["Port"]+")/"+
                cfg.MetricDB["Database"]
    mdb_conn, err := sql.Open("mysql", mdb_dsn)
    if err != nil {
        log.Fatal("Cannot connect to the Policy database:", err)
    }
    // defer mdb_conn.Close()

    // HistoryDB connection
    // postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
    hdb_dsn :=  "postgresql://"+
                cfg.HistoryDB["Username"]+":"+
                cfg.HistoryDB["Password"]+"@"+
                cfg.HistoryDB["Host"]+":"+
                cfg.HistoryDB["Port"]+"/"+
                cfg.HistoryDB["Database"]
    hdb_conn, err := sql.Open("postgres", hdb_dsn)
    if err != nil {
        log.Fatal("Cannot connect to the Policy database:", err)
    }
    // defer hdb_conn.Close()

    pdb := PolicyDB{db: pdb_conn}
    mdb := MetricDB{db: mdb_conn}
    hdb := HistoryDB{db: hdb_conn}

    api = API {
        pdb: &pdb,
        mdb: &mdb,
        hdb: &hdb}

}

func main() {
    r := mux.NewRouter()
    r.HandleFunc("/", IndexHandler)

    // app api
    r.HandleFunc("/apps", ListAppsHandler).Methods("GET")
    r.HandleFunc("/apps", PostAppHandler).Methods("POST")
    r.HandleFunc("/apps/{app_uuid}", GetAppHandler).Methods("GET")
    r.HandleFunc("/apps/{app_uuid}", PutAppHandler).Methods("PUT")

    // history api
    r.HandleFunc("/apps/{app_uuid}/history", GetHistoryHandler).Methods("GET")
    r.HandleFunc("/apps/{app_uuid}/metric", GetMetricHandler).Methods("GET")
    r.HandleFunc("/apps/{app_uuid}/metric/avg", GetAvgMetricHandler).Methods("GET")

    // tuna
    // policy api
    r.HandleFunc("/policies/{app_uuid}", ListPoliciesHandler).Methods("GET")
    r.HandleFunc("/policies/{app_uuid}", PostPolicyHandler).Methods("POST")
    r.HandleFunc("/policies/{app_uuid}/{policy_uuid}", PutPolicyHandler).Methods("PUT")
    r.HandleFunc("/policies/{app_uuid}/{policy_uuid}", GetPolicyHandler).Methods("GET")
    // r.HandleFunc("/apps/{app_uuid}/policy/{id}", DeletePolicyHandler).Methods("DELETE")

    // cron api
    r.HandleFunc("/crons/{app_uuid}", ListCronsHandler).Methods("GET")
    r.HandleFunc("/crons/{app_uuid}", PostCronHandler).Methods("POST")
    r.HandleFunc("/crons/{app_uuid}/{cron_uuid}", PutCronHandler).Methods("PUT")
    r.HandleFunc("/crons/{app_uuid}/{cron_uuid}", GetCronHandler).Methods("GET")

    // end tuna
    http.Handle("/", r)

    err := http.ListenAndServe(":" + cfg.Port, nil)
    if err != nil {
        log.Fatal("Cannot start the server: ", err)
    }
}

func IndexHandler(w http.ResponseWriter, r *http.Request) {
    fmt.Fprint(w, "IndexHandler is here")
}

// chanhlv
func ListAppsHandler(w http.ResponseWriter, r *http.Request) {
    var apps []Application
    // vars := mux.Vars(r)       
 
    apps, err := api.pdb.GetApps()
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }
 
    app_json, err := json.Marshal(apps)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }
 
    w.Write(app_json)
}
// end chanhlv

func PostAppHandler(w http.ResponseWriter, r *http.Request) {
    var app Application

    err := json.NewDecoder(r.Body).Decode(&app)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }
    
    exist, err := api.pdb.IsExistApp(app.App_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist {
        http.Error(w, ErrExisting, http.StatusBadRequest)
        return
    }

    err = api.pdb.AddApp(app)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }

    fmt.Fprint(w, SuccessMsg, http.StatusCreated)
}

func PutAppHandler(w http.ResponseWriter, r *http.Request) {
    var app Application
    vars := mux.Vars(r)
    app.App_uuid = vars["app_uuid"]

    exist, err := api.pdb.IsExistApp(app.App_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist == false {
        http.Error(w, ErrNotExist, http.StatusBadRequest)
        return
    }

    err = json.NewDecoder(r.Body).Decode(&app)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }
    
    err = api.pdb.UpdateApp(app)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }

    fmt.Fprint(w, SuccessMsg, http.StatusOK)
}

func GetAppHandler(w http.ResponseWriter, r *http.Request) {
    var app Application
    vars := mux.Vars(r)
    app_uuid := vars["app_uuid"]

    exist, err := api.pdb.IsExistApp(app_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist == false {
        http.Error(w, ErrNotExist, http.StatusBadRequest)
        return
    }

    app, err = api.pdb.GetApp(app_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    app_json, err := json.Marshal(app)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    w.Write(app_json)
}

//tuna

func ListPoliciesHandler(w http.ResponseWriter, r *http.Request) {
    // var policy_ids []int
    var policies []Policy
    w.Header().Add("Access-Control-Allow-Origin", "*")
    w.Header().Add("Content-Type", "application/json")
    w.Header().Add("Access-Control-Allow-Methods", "GET")
    w.Header().Add("Access-Control-Allow-Credentials", "true")
    w.Header().Add("Access-Control-Max-Age", "1728000")

    vars := mux.Vars(r)       
    app_uuid := vars["app_uuid"]

    policies, err := api.pdb.GetPolicies(app_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }
 
    policy_json, err := json.Marshal(policies)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }
    w.Header().Set("Access-Control-Allow-Origin", "*")
    w.Write(policy_json)
}

func PutPolicyHandler(w http.ResponseWriter, r *http.Request) {
    var policy Policy
    var exist bool
    var err error

    vars := mux.Vars(r)
    policy.App_uuid = vars["app_uuid"]
    policy.Policy_uuid = vars["policy_uuid"]

    // check existing app
    exist, err = api.pdb.IsExistApp(policy.App_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist == false {
        http.Error(w, ErrNotExist, http.StatusBadRequest)
        return
    }

    // check existing policy
    exist, err = api.pdb.IsExistPolicy(policy.Policy_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist == false {
        http.Error(w, ErrNotExist, http.StatusBadRequest)
        return
    }

    // decode json parameters
    err = json.NewDecoder(r.Body).Decode(&policy)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }
    
    // update policy
    err = api.pdb.UpdatePolicy(policy)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }

    fmt.Fprint(w, SuccessMsg, http.StatusOK)
}

func PostPolicyHandler(w http.ResponseWriter, r *http.Request) {
    var policy Policy
    var exist bool
    var err error

    err = json.NewDecoder(r.Body).Decode(&policy)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }
    // check existing policy
    exist, err = api.pdb.IsExistPolicy(policy.Policy_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist {
        http.Error(w, ErrExisting, http.StatusBadRequest)
        return
    }

    // add policy
    err = api.pdb.AddPolicy(policy)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }

    fmt.Fprint(w, SuccessMsg, http.StatusCreated)
}

func GetPolicyHandler(w http.ResponseWriter, r *http.Request) {
    var policy Policy
    var exist bool
    var err error

    vars := mux.Vars(r)
    app_uuid := vars["app_uuid"]
    policy_uuid := vars["policy_uuid"]

    // check existing app
    exist, err = api.pdb.IsExistApp(app_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist == false {
        http.Error(w, ErrNotExist, http.StatusBadRequest)
        return
    }

    // check existing policy
    exist, err = api.pdb.IsExistPolicy(policy_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist == false {
        http.Error(w, ErrNotExist, http.StatusBadRequest)
        return
    }

    // get policy
    policy, err = api.pdb.GetPolicy(policy_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    policy_json, err := json.Marshal(policy)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    w.Write(policy_json)
}

func ListCronsHandler(w http.ResponseWriter, r *http.Request) {
    var crons []Crontab
    vars := mux.Vars(r)       
    app_uuid := vars["app_uuid"]

    crons, err := api.pdb.GetCrons(app_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }
 
    cron_json, err := json.Marshal(crons)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }
 
    w.Write(cron_json)
}

func PutCronHandler(w http.ResponseWriter, r *http.Request) {
    var crontab Crontab
    var exist bool
    var err error

    vars := mux.Vars(r)
    crontab.App_uuid = vars["app_uuid"]
    crontab.Cron_uuid = vars["cron_uuid"]

    // check existing app
    exist, err = api.pdb.IsExistApp(crontab.App_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist == false {
        http.Error(w, ErrNotExist, http.StatusBadRequest)
        return
    }

    // check existing cron
    exist, err = api.pdb.IsExistCron(crontab.Cron_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist == false {
        http.Error(w, ErrNotExist, http.StatusBadRequest)
        return
    }

    // decode json parameters
    err = json.NewDecoder(r.Body).Decode(&crontab)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }
    
    // update cron
    err = api.pdb.UpdateCron(crontab)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }

    fmt.Fprint(w, SuccessMsg, http.StatusOK)
}

func PostCronHandler(w http.ResponseWriter, r *http.Request) {
    var crontab Crontab
    var exist bool
    var err error
    var app Application

    err = json.NewDecoder(r.Body).Decode(&crontab)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }
    // check existing app
    exist, err = api.pdb.IsExistApp(crontab.App_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist == false {
        http.Error(w, ErrExisting, http.StatusBadRequest)
        return
    }
    // check existing cron
    exist, err = api.pdb.IsExistPolicy(crontab.Cron_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist {
        http.Error(w, ErrExisting, http.StatusBadRequest)
        return
    }

    // add cron
    err = api.pdb.AddCron(crontab)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrInvalidParam, http.StatusBadRequest)
        return
    }

    // create new cron using cron library
    c := cron.New()
    c.AddFunc(crontab.Cron_string, func() {
            app.App_uuid = crontab.App_uuid
            app.Min_instances = crontab.Min_instances
            app.Max_instances = crontab.Max_instances
            log.Println("Tu dep trai")

            // check existing app
            exist, err := api.pdb.IsExistApp(app.App_uuid)
            if err != nil {
                log.Println(err)
                http.Error(w, ErrServerFailed, http.StatusInternalServerError)
                return
            }

            if exist == false {
                http.Error(w, ErrNotExist, http.StatusBadRequest)
                return
            }

            err = api.pdb.UpdateApp(app)
            if err != nil {
                log.Println(err)
                http.Error(w, ErrInvalidParam, http.StatusBadRequest)
                return
            }
        })
    c.Start()

    fmt.Fprint(w, SuccessMsg, http.StatusCreated)
}

func GetCronHandler(w http.ResponseWriter, r *http.Request) {
    var crontab Crontab
    var exist bool
    var err error

    vars := mux.Vars(r)
    app_uuid := vars["app_uuid"]
    cron_uuid := vars["cron_uuid"]

    // check existing app
    exist, err = api.pdb.IsExistApp(app_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist == false {
        http.Error(w, ErrNotExist, http.StatusBadRequest)
        return
    }

    // check existing cron
    exist, err = api.pdb.IsExistCron(cron_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    if exist == false {
        http.Error(w, ErrNotExist, http.StatusBadRequest)
        return
    }

    // get cron
    crontab, err = api.pdb.GetCron(cron_uuid)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    cron_json, err := json.Marshal(crontab)
    if err != nil {
        log.Println(err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    w.Write(cron_json)
}
// end tuna
// histories 

func GetHistoryHandler(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    r.ParseForm()
    app_uuid := vars["app_uuid"]
    var err error

    var start int // default: from the beginning
    if i, ok := r.Form["start"]; ok {
        start, err = strconv.Atoi(i[0])
        if err != nil {
            http.Error(w, ErrInvalidParam, http.StatusBadRequest)
            return
        }
    }  

    var end = int(time.Now().Unix()) // default: now
    if i, ok := r.Form["end"]; ok {
        end, err = strconv.Atoi(i[0])
        if err != nil {
            http.Error(w, ErrInvalidParam, http.StatusBadRequest)
            return
        }        
    }

    histories, err := api.hdb.Get(app_uuid, start, end)
    if err != nil {
        log.Fatal("Error occurs when getting history: ", err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    result, err := json.Marshal(histories)
    if err != nil {
        log.Fatal("Error occurs when marshaling: ", err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    w.Write(result)
}

func GetMetricHandler(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    r.ParseForm()
    app_uuid := vars["app_uuid"]
    var err error

    var start int // default: from the beginning
    if i, ok := r.Form["start"]; ok {
        start, err = strconv.Atoi(i[0])
        if err != nil {
            http.Error(w, ErrInvalidParam, http.StatusBadRequest)
            return
        }
    }  

    var end = int(time.Now().Unix()) // default: now
    if i, ok := r.Form["end"]; ok {
        end, err = strconv.Atoi(i[0])
        if err != nil {
            http.Error(w, ErrInvalidParam, http.StatusBadRequest)
            return
        }        
    }

    var instance_uuid string // default: any instances
    if i, ok := r.Form["instance_uuid"]; ok {
        instance_uuid = i[0]
    }

    metrics, err := api.mdb.Get(app_uuid, start, end, instance_uuid)
    if err != nil {
        log.Fatal("Error occurs when getting metric: ", err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    result, err := json.Marshal(metrics)
    if err != nil {
        log.Fatal("Error occurs when marshaling: ", err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    w.Write(result)
}

func GetAvgMetricHandler(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    r.ParseForm()
    app_uuid := vars["app_uuid"]
    var err error

    var start int // default: from the beginning
    if i, ok := r.Form["start"]; ok {
        start, err = strconv.Atoi(i[0])
        if err != nil {
            http.Error(w, ErrInvalidParam, http.StatusBadRequest)
            return
        }
    }  

    var end = int(time.Now().Unix()) // default: now
    if i, ok := r.Form["end"]; ok {
        end, err = strconv.Atoi(i[0])
        if err != nil {
            http.Error(w, ErrInvalidParam, http.StatusBadRequest)
            return
        }        
    }

    var step int = 60 // seconds
    metrics, err := api.mdb.GetAvg(app_uuid, start, end, step)
    if err != nil {
        log.Fatal("Error occurs when getting metric: ", err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    result, err := json.Marshal(metrics)
    if err != nil {
        log.Fatal("Error occurs when marshaling: ", err)
        http.Error(w, ErrServerFailed, http.StatusInternalServerError)
        return
    }

    w.Write(result)
}