package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
)

var (
	manager *Manager
	bind    = flag.String("bind", ":8888", "bind")
)

func main() {
	flag.Parse()
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 2 * time.Second,
	})

	if err != nil {
		panic(err)
	}
	defer etcdCli.Close()

	etcdSession, err := concurrency.NewSession(etcdCli, concurrency.WithTTL(10))
	if err != nil {
		panic(err)
	}
	defer etcdSession.Close()
	manager = NewManager(etcdSession)

	http.HandleFunc("/task/add", addTaskHandler)
	http.HandleFunc("/task/del", delTaskHandler)
	log.Fatal(http.ListenAndServe(*bind, nil))
}
