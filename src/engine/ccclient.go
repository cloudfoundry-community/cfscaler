package main 

import (
    "errors"
    "net/http"
    "log"
    "strings"
    "io/ioutil"
    "encoding/json"
)

type CCClient struct {
    api_host string
    auth_host string
    auth_user string
    auth_pass string
}

type Token struct {
    Access_token string
    Token_type string
    Refresh_token string
    Expires_in int
    Scope string 
    Jti string
}

type App struct {
    Name string
    Instances int
}

func (c *CCClient) ScaleOut(app_uuid string, num int, max int) (num_after int, err error) {
    errMaximum := errors.New("Already at maximum number of instances")

    num_current, err := c.getNumInstances(app_uuid)
    if err != nil {
        log.Println("Error occurs when getting number of instances: ", err)
        return 0, err
    }
    if num_current > max { // It happens when users did manual scaling
        return num_current, errMaximum
    }
    if num_current == max {
        return max, errMaximum
    }
    if num_current + num > max {
        err := c.setNumInstances(app_uuid, max)
        if err != nil {
            log.Println("Error occurs when scaling out: ", err)
            return num_current, err
        }
        return max, nil
    }
    err1 := c.setNumInstances(app_uuid, num_current + num)
    if err1 != nil {
        log.Println("Error occurs when scaling out: ", err)
        return num_current, err
    }
    return num_current + num, nil
}

func (c *CCClient) ScaleIn(app_uuid string, num int, min int) (num_after int, err error) {
    errMinimum := errors.New("Already at minimum number of instances")

    num_current, err := c.getNumInstances(app_uuid)
    if err != nil {
        log.Println("Error occurs when getting number of instances: ", err)
        return 0, err
    }
    if num_current < min { // It happens when users did manual scaling
        return num_current, errMinimum
    }
    if num_current == min {
        return min, errMinimum
    }
    if num_current - num < min {
        err := c.setNumInstances(app_uuid, min)
        if err != nil {
            log.Println("Error occurs when scaling out: ", err)
            return num_current, err
        }
        return min, nil
    }
    err1 := c.setNumInstances(app_uuid, num_current - num)
    if err1 != nil {
        log.Println("Error occurs when scaling out: ", err)
        return num_current, err
    }
    return num_current - num, nil
}

func (c *CCClient) getNumInstances(app_uuid string) (num int, err error) {
    API_URI := strings.Join([]string{"http://", c.api_host, "/v2/apps/", app_uuid, "/summary"}, "")
    
    req, err := http.NewRequest("GET", API_URI, strings.NewReader(""))
    if err != nil {
        log.Println("Cannot create GET request to the API host.")
        return 0, err
    }
    token, err := c.getToken()
    if err != nil {
        log.Println("Cannot get token.")
        return 0, err
    }
    req.Header.Add("Authorization", "Bearer " + token)
    req.Header.Add("Content-Type", "application/json")
    req.Header.Add("Accept", "application/json")

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        log.Println("Request to the API host failed.")
        return 0, err
    }
    defer resp.Body.Close()
    // TODO: response code != 200

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        log.Println("Cannot read the response from API server.")
        return 0, err
    }
    
    var app App
    tmp_err := json.Unmarshal(body, &app)
    if tmp_err != nil {
        log.Println("Cannot decode JSON message from API server.")
        return 0, err
    }

    return app.Instances, nil
}

func (c *CCClient) setNumInstances(app_uuid string, num int) error {
    API_URI := strings.Join([]string{"http://", c.api_host, "/v2/apps/", app_uuid}, "")
    
    req_body, err := json.Marshal(map[string]int{"instances": num})
    if err != nil {
        log.Println("Cannot encode request body.")
        return err
    }

    req, err := http.NewRequest("PUT", API_URI, strings.NewReader(string(req_body)))
    if err != nil {
        log.Println("Cannot create PUT request to the API server.")
        return err
    }

    token, err := c.getToken()
    if err != nil {
        log.Println("Cannot get access token.")
        return err
    }

    req.Header.Add("Authorization", "Bearer " + token)
    req.Header.Add("Content-Type", "application/json")
    req.Header.Add("Accept", "application/json")

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        log.Println("Request to API server failed.")
        return err
    }
    defer resp.Body.Close()
    // TODO: response code != 200

    return nil
}

func (c *CCClient) getToken() (token string, err error) {
    // TODO: Use refresh_token
    auth_URI := strings.Join([]string{"http://", c.auth_host, "/oauth/token"}, "")
    data := strings.Join([]string{"grant_type=password&username=", c.auth_user, "&password=", c.auth_pass}, "")
    req, err := http.NewRequest("POST", auth_URI, strings.NewReader(data))
    if err != nil {
        log.Println("Cannot create POST request to the authentication server.")
        return "", err
    }

    req.Header.Add("Authorization", "Basic Y2Y6")
    req.Header.Add("Accept", "application/json, application/x-www-form-urlencoded")
    req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        log.Println("Request to the authentication failed.")
        return "", err
    }
    defer resp.Body.Close()
    // TODO: response code != 200

    body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        log.Println("Cannot read response from the authentication server.")
        return "", err
    }
    var t Token
    tmp_err := json.Unmarshal(body, &t)
    if tmp_err != nil {
        log.Println("Decode JSON failed.")
        return "", err
    }
    return t.Access_token, nil
}

// func main() {
//     app_uuid := "ea2fd84c-2c5d-432d-b27c-3a3005cd9ba8"

//     c := CCClient {
//         api_host: "api.10.16.180.40.xip.io",
//         auth_host: "login.10.16.180.40.xip.io",
//         auth_user: "admin",
//         auth_pass: "admin",
//     }
    

//     num, err := c.getNumInstances(app_uuid)
//     if err != nil {
//         log.Fatal("Fail", err)
//     }

//     fmt.Println(num)
//     err1 := c.setNumInstances(app_uuid, num - 1)
//     if err1 != nil {
//         log.Fatal("Fail2", err1)
//     }
//     fmt.Println(c.getNumInstances(app_uuid))
// }