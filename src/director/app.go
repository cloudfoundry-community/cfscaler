package main 

type App struct {
    App_uuid string
    Name string
    Min_instances int 
    Max_instances int
    Policies []Policy
}