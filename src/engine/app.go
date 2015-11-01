package main 

type Application struct {
    App_uuid string
    Name string
    Min_instances int 
    Max_instances int
    Policies []Policy
}