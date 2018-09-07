package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

var (
	tasks sync.Map
	quota = flag.String("quota", "10", "example quota")
)

func registerWorker(cli *clientv3.Client) string {
	workerUUID := uuid.Must(uuid.NewV4()).String()

	resp, err := cli.Grant(context.TODO(), 2)
	if err != nil {
		panic(err)
	}

	ch, err := cli.KeepAlive(context.TODO(), resp.ID)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			<-ch
		}
	}()

	_, err = cli.Put(context.TODO(), "/workers/"+workerUUID, *quota, clientv3.WithLease(resp.ID))
	if err != nil {
		panic(err)
	}

	return workerUUID
}

func watchUpdate(cli *clientv3.Client, workerUUID string) {
	prefix := "/procs/" + workerUUID + "/"
	wChan := cli.Watch(context.TODO(), prefix, clientv3.WithPrefix())

	for wresp := range wChan {
		for _, ev := range wresp.Events {
			taskName := strings.TrimPrefix(string(ev.Kv.Key), prefix)
			switch ev.Type {
			case mvccpb.PUT:
				fmt.Printf("NEW TASK %q : %q\n", taskName, ev.Kv.Value)
				tasks.Store(taskName, string(ev.Kv.Value))
			case mvccpb.DELETE:
				fmt.Printf("DELETE TASK %q\n", taskName)
				tasks.Delete(taskName)
			}
		}
	}
}

func main() {
	flag.Parse()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://localhost:2379"},
		DialTimeout: 2 * time.Second,
	})

	if err != nil {
		panic(err)
	}
	defer cli.Close()

	workerUUID := registerWorker(cli)
	watchUpdate(cli, workerUUID)
}
