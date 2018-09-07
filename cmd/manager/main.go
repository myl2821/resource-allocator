package main

import (
	"log"
	"net/http"
	"time"

	"go.etcd.io/etcd/clientv3"
)

var manager *Manager

func main() {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 2 * time.Second,
	})

	if err != nil {
		panic(err)
	}
	defer etcdCli.Close()

	manager = NewManager(etcdCli)

	http.HandleFunc("/task/add", addTaskHandler)
	http.HandleFunc("/task/del", delTaskHandler)
	log.Fatal(http.ListenAndServe(":8888", nil))
}
