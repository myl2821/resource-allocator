package main

import (
	"io/ioutil"
	"net/http"
	"time"
)

// addTaskHandler deals with dummy task
func addTaskHandler(w http.ResponseWriter, r *http.Request) {
	payload, _ := ioutil.ReadAll(r.Body)
	task := string(payload)
	manager.newTask(time.Now(), task)

	w.WriteHeader(200)
}

// delTaskHandler deals with dummy task
func delTaskHandler(w http.ResponseWriter, r *http.Request) {
	payload, _ := ioutil.ReadAll(r.Body)
	task := string(payload)
	manager.deleteTask(task)

	w.WriteHeader(200)
}
