package main

import (
    "database/sql"
    "log"
    "errors"
    "strconv"
    // "time"
 )
    
type PolicyDB struct {
    db *sql.DB
}

type Policy struct {
    // tuna
    Policy_uuid string
    App_uuid string
    // end tuna
    Metric_type int 
    Upper_threshold float64
    Lower_threshold float64
    Instances_out int
    Instances_in int
    Cooldown_period int
    Measurement_period int
    // tuna
    Deleted bool
    // end tuna
}

type Application struct {
    App_uuid string
    Name string
    Min_instances int 
    Max_instances int
    Enabled bool
}

// tuna
type Crontab struct {
    App_uuid string
    Cron_uuid string
    Min_instances int
    Max_instances int
    Cron_string string
    Deleted bool 

}
// end tuna

func (pdb *PolicyDB) IsExistApp(app_uuid string) (bool, error) {
    rows, err := pdb.db.Query("SELECT Id FROM apps WHERE app_uuid = ?", app_uuid)
    if err != nil {
        log.Println("Error occurs when querying database:", err)
        return false, err
    }
    defer rows.Close()

    if rows.Next() {
        return true, nil
    } else {
        return false, nil
    }
}

func (pdb *PolicyDB) GetApp(app_uuid string) (Application, error) {
    var app Application
    log.Println(app_uuid)
    err := pdb.db.QueryRow("SELECT app_uuid, name, min_instances, max_instances, enabled FROM apps WHERE app_uuid = ?", app_uuid).Scan(&app.App_uuid, &app.Name, &app.Min_instances, &app.Max_instances, &app.Enabled)
    if err != nil {
        log.Println("Error occurs when getting application:", err)
        return app, err
    }

    return app, nil
}
// chanhlv
func (pdb *PolicyDB) GetApps() ([]Application, error) {
    var apps []Application
    rows, err := pdb.db.Query("SELECT app_uuid, name, min_instances, max_instances, enabled FROM apps")
    if err != nil {
        log.Println("Error occurs when querying database:", err)
    }
    defer rows.Close()
 
    for rows.Next() {
        var app Application
        err = rows.Scan(&app.App_uuid, &app.Name, &app.Min_instances, &app.Max_instances, &app.Enabled)
        if err != nil {
            panic(err.Error())
        }
        apps = append(apps, app)
    }
    return apps, err
}


// end chanhlv

func (pdb *PolicyDB) AddApp(app Application) error {
    if app.App_uuid == "" {
        return errors.New("App_uuid is missing")
    }
    if app.Name == "" {
        return errors.New("Name is missing")
    }
    if app.Min_instances == 0 {
        app.Min_instances = 1
    }
    if app.Max_instances == 0 {
        app.Max_instances = 5
    }

    _, err := pdb.db.Exec("INSERT INTO apps(app_uuid, name, min_instances, max_instances, enabled) VALUES (?, ?, ?, ?, ?)", app.App_uuid, app.Name, app.Min_instances, app.Max_instances, app.Enabled)
    if err != nil {
        return err
    }

    return nil
}

func (pdb *PolicyDB) UpdateApp(app Application) error {
    q := "UPDATE apps SET "
    if app.Name != "" {
        q = q + " name = '" + app.Name + "'" + ", "
    }
    if app.Min_instances != 0 {
        q = q + "min_instances = " + strconv.Itoa(app.Min_instances) + ", "
    }
    if app.Max_instances != 0 {
        q = q + "max_instances = " + strconv.Itoa(app.Max_instances) + ", "
    }
    q = q + "enabled = " + strconv.FormatBool(app.Enabled)
    q = q + " WHERE app_uuid = '" + app.App_uuid + "'"

    _, err := pdb.db.Exec(q)
    if err != nil {
        return err
    }

    return nil
}

// tuna
func (pdb *PolicyDB) IsExistCron(cron_uuid string) (bool, error) {
    rows, err := pdb.db.Query("SELECT Id FROM crons WHERE cron_uuid = ?", cron_uuid)
    if err != nil {
        log.Println("Error occurs when querying database:", err)
        return false, err
    }
    defer rows.Close()

    if rows.Next() {
        return true, nil
    } else {
        return false, nil
    }
}

func (pdb *PolicyDB) AddCron(crontab Crontab) error {
    if crontab.App_uuid == "" {
        return errors.New("App_uuid is missing")
    }
    if crontab.Cron_uuid == "" {
        return errors.New("Cron_uuid is missing")
    }

    _, err := pdb.db.Exec("INSERT INTO crons(app_uuid, cron_uuid, min_instances, max_instances, cron_string, deleted) VALUES (?, ?, ?, ?, ?, ?)", crontab.App_uuid, crontab.Cron_uuid, crontab.Min_instances, crontab.Max_instances, crontab.Cron_string, crontab.Deleted)
    if err != nil {
        return err
    }

    return nil
}

func (pdb *PolicyDB) GetCron(cron_uuid string) (Crontab, error) {
    var crontab Crontab
    err := pdb.db.QueryRow("SELECT app_uuid, cron_uuid, min_instances, max_instances, cron_string, deleted FROM crons WHERE cron_uuid = ?", cron_uuid).Scan(&crontab.App_uuid, &crontab.Cron_uuid, &crontab.Min_instances, &crontab.Max_instances, &crontab.Cron_string, &crontab.Deleted)
    if err != nil {
        log.Println("Error occurs when getting cron job:", err)
        return crontab, err
    }

    return crontab, nil
}

func (pdb *PolicyDB) UpdateCron(crontab Crontab) error {
    q := "UPDATE crons SET "
    if crontab.Min_instances != 0 {
        q = q + " min_instances = " + strconv.Itoa(crontab.Min_instances) + ", "
    }
    if crontab.Max_instances != 0 {
        q = q + " max_instances = " + strconv.Itoa(crontab.Max_instances) + ", "
    }

    q = q + "cron_string = '" + crontab.Cron_string + "', "

    q = q + "deleted = " + strconv.FormatBool(crontab.Deleted)
    q = q + " WHERE cron_uuid = '" + crontab.Cron_uuid + "'"

    _, err := pdb.db.Exec(q)
    if err != nil {
        return err
    }

    return nil
}

func (pdb *PolicyDB) GetCrons(app_uuid string) ([]Crontab, error) {
    var crons []Crontab
    rows, err := pdb.db.Query("SELECT app_uuid, cron_uuid, min_instances, max_instances, cron_string, deleted FROM crons WHERE app_uuid = ? AND deleted = false", app_uuid )
    if err != nil {
        log.Println("Error occurs when getting cron:", err)
    }
    defer rows.Close()

    for rows.Next() {
        var crontab Crontab
        err = rows.Scan(&crontab.App_uuid, &crontab.Cron_uuid, &crontab.Min_instances, &crontab.Max_instances, &crontab.Cron_string, &crontab.Deleted)
        if err != nil {
            panic(err.Error())
        }
        crons = append(crons, crontab)
    }
    return crons, err
}

func (pdb *PolicyDB) IsExistPolicy(policy_uuid string) (bool, error) {
    rows, err := pdb.db.Query("SELECT Id FROM policies WHERE policy_uuid = ?", policy_uuid)
    if err != nil {
        log.Println("Error occurs when querying database:", err)
        return false, err
    }
    defer rows.Close()

    if rows.Next() {
        return true, nil
    } else {
        return false, nil
    }
}


func (pdb *PolicyDB) AddPolicy(policy Policy) error {
    if policy.App_uuid == "" {
        return errors.New("App_uuid is missing")
    }
    if policy.Policy_uuid == "" {
        return errors.New("Policy_uuid is missing")
    }

    _, err := pdb.db.Exec("INSERT INTO policies(app_uuid, policy_uuid, metric_type, upper_threshold, lower_threshold, instances_out, instances_in, cooldown_period, measurement_period, deleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", policy.App_uuid, policy.Policy_uuid, policy.Metric_type, policy.Upper_threshold, policy.Lower_threshold, policy.Instances_out, policy.Instances_in, policy.Cooldown_period, policy.Measurement_period, policy.Deleted)
    if err != nil {
        return err
    }

    return nil
}

func (pdb *PolicyDB) UpdatePolicy(policy Policy) error {
    q := "UPDATE policies SET "
    if policy.Metric_type != 0 {
        q = q + "metric_type = " + strconv.Itoa(policy.Metric_type) + ", "
    }
    if policy.Upper_threshold != 0 {
        q = q + "upper_threshold = " + strconv.FormatFloat(policy.Upper_threshold, 'f', 6, 64) + ", "
    }
    if policy.Lower_threshold != 0 {
        q = q + "lower_threshold = " + strconv.FormatFloat(policy.Lower_threshold, 'f', 6, 64) + ", "
    }
    if policy.Instances_out != 0 {
        q = q + "instances_out = " + strconv.Itoa(policy.Instances_out) + ", "
    }
    if policy.Instances_in != 0 {
        q = q + "instances_in = " + strconv.Itoa(policy.Instances_in) + ", "
    }
    if policy.Cooldown_period != 0 {
        q = q + "cooldown_period = " + strconv.Itoa(policy.Cooldown_period) + ", "
    }
    if policy.Measurement_period != 0 {
        q = q + "measurement_period = " + strconv.Itoa(policy.Measurement_period) + ", "
    }

    q = q + " deleted = " + strconv.FormatBool(policy.Deleted)
    q = q + " WHERE policy_uuid = '" + policy.Policy_uuid + "'"

    _, err := pdb.db.Exec(q)
    if err != nil {
        return err
    }

    return nil
}

func (pdb *PolicyDB) GetPolicy(policy_uuid string) (Policy, error) {
    var policy Policy
    err := pdb.db.QueryRow("SELECT app_uuid, policy_uuid, metric_type, upper_threshold, lower_threshold, instances_out, instances_in, cooldown_period, measurement_period, deleted FROM policies WHERE policy_uuid = ?", policy_uuid).Scan(&policy.App_uuid, &policy.Policy_uuid, &policy.Metric_type, &policy.Upper_threshold, &policy.Lower_threshold, &policy.Instances_out, &policy.Instances_in, &policy.Cooldown_period, &policy.Measurement_period, &policy.Deleted)
    if err != nil {
        log.Println("Error occurs when getting policy:", err)
        return policy, err
    }

    return policy, nil
}

func (pdb *PolicyDB) GetPolicies(app_uuid string) ([]Policy, error) {
    var policies []Policy
    rows, err := pdb.db.Query("SELECT app_uuid, policy_uuid, metric_type, upper_threshold, lower_threshold, instances_out, instances_in, cooldown_period, measurement_period, deleted FROM policies WHERE app_uuid = ? AND deleted = false", app_uuid)
    if err != nil {
        log.Println("Error occurs when getting policy:", err)
    }
    defer rows.Close()

    for rows.Next() {
        var policy Policy
        err = rows.Scan(&policy.App_uuid, &policy.Policy_uuid, &policy.Metric_type, &policy.Upper_threshold, &policy.Lower_threshold, &policy.Instances_out, &policy.Instances_in, &policy.Cooldown_period, &policy.Measurement_period, &policy.Deleted)
        if err != nil {
            panic(err.Error())
        }
        policies = append(policies, policy)
    }
    return policies, err
}
// end tuna