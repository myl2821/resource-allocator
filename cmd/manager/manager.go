package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

const (
	learderPath   = "/leader"
	workerPrefix  = "/workers/"
	procPrefix    = "/procs/"
	pendingPrefix = "/tasks/pending/"
	runningPrefix = "/tasks/running/"
)

func init() {
	rand.Seed(int64(time.Now().Nanosecond()))
}

// A Worker is defined by its IP uuid and quota
type Worker struct {
	manager *Manager
	uuid    string
	quota   int
}

func (w *Worker) procs() ([]string, error) {
	procsResp, err := w.manager.cli.Get(context.TODO(), procPrefix+w.uuid+"/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	procs := make([]string, procsResp.Count)

	for i, kv := range procsResp.Kvs {
		procs[i] = string(kv.Key)
	}

	return procs, nil
}

// Manager manages workers and allocate tasks by quota
// it also re-allocate task when one worker fails
type Manager struct {
	sync.Mutex

	cli      *clientv3.Client
	session  *concurrency.Session
	isLeader bool
}

// NewManager returns a new manager
func NewManager(session *concurrency.Session) *Manager {
	manager := &Manager{
		session:  session,
		cli:      session.Client(),
		isLeader: false,
	}
	go manager.leaderElect()
	go manager.watchLeader()
	go manager.watchWorker()

	manager.allocatePendingTasks()

	return manager
}

func (m *Manager) workers() ([]Worker, error) {
	workersResp, err := m.cli.Get(context.TODO(), workerPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	workers := make([]Worker, workersResp.Count)

	for i, kv := range workersResp.Kvs {
		quota, _ := strconv.Atoi(string(kv.Value))
		workers[i] = Worker{
			manager: m,
			uuid:    strings.TrimPrefix(string(kv.Key), workerPrefix),
			quota:   quota,
		}
	}

	return workers, nil
}

func (m *Manager) watchLeader() {
	wChan := m.cli.Watch(context.TODO(), learderPath)

	for wresp := range wChan {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case mvccpb.DELETE:
				m.leaderElect()
			}
		}
	}
}

// leaderElect elects for itself if no leader existed in quorum
func (m *Manager) leaderElect() {
reDo:
	resp, err := m.cli.Get(context.TODO(), learderPath)
	if err != nil {
		panic(err)
	}

	if resp.Count == 0 {
		isLeader, err := m.electSelf()
		if err != nil {
			panic(err)
		}
		if isLeader {
			m.isLeader = true
			fmt.Println("become leader")
			return
		}

		sleepMs := time.Duration(int64(50 + rand.Int31n(50)))
		time.Sleep(sleepMs)
		goto reDo
	}

	return
}

func (m *Manager) electSelf() (bool, error) {
	resp, err := m.cli.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.CreateRevision(learderPath), "=", 0)).
		Then(clientv3.OpPut(learderPath, "1", clientv3.WithLease(m.session.Lease()))).
		Commit()

	if err != nil {
		return false, err
	}

	return resp.Succeeded, nil
}

// watchWorker watches update in workers configuration
func (m *Manager) watchWorker() {
	wChan := m.cli.Watch(context.TODO(), workerPrefix, clientv3.WithPrefix())

	for wresp := range wChan {
		for _, ev := range wresp.Events {
			uuid := string(ev.Kv.Key)
			if !m.isLeader {
				continue
			}
			switch ev.Type {
			case mvccpb.PUT:
				fmt.Printf("PUT %q : %q\n", uuid, ev.Kv.Value)
				m.newWorkerHandler()
			case mvccpb.DELETE:
				fmt.Printf("DELETE %q : %q\n", string(ev.Kv.Key), string(ev.Kv.Value))
				m.delWorkerHandler(uuid)
			}
		}
	}
}

func (m *Manager) allocatePendingTasks() error {
	pendingResp, err := m.cli.Get(context.TODO(), pendingPrefix,
		clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByValue, clientv3.SortAscend))

	if err != nil {
		return err
	}

	for _, pending := range pendingResp.Kvs {
		task := strings.TrimPrefix(string(pending.Key), pendingPrefix)
		err = m.allocateOneTask(task, string(pending.Value))
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) allocateOneTask(task string, ts string) error {
	minUtilzation := float32(1.0)
	allWorkerFull := true

	workers, err := m.workers()
	if err != nil {
		return err
	}

	if len(workers) == 0 {
		return nil
	}

	var workerToAllcate Worker

	for _, worker := range workers {
		procs, err := worker.procs()
		if err != nil {
			return err
		}

		util := float32(len(procs)) / float32(worker.quota)

		if util < minUtilzation {
			minUtilzation = util
			workerToAllcate = worker
			allWorkerFull = false
		}
	}

	if allWorkerFull {
		return errors.New("all worker full")
	}

	// move task from pending to running state, and bookkeeping it to worker's procs
	_, err = m.cli.Txn(context.TODO()).
		Then(
			clientv3.OpDelete(pendingPrefix+task),
			clientv3.OpPut(runningPrefix+task, workerToAllcate.uuid+"$$$"+ts),
			clientv3.OpPut(procPrefix+workerToAllcate.uuid+"/"+task, ts)).
		Commit()

	return nil
}

func (m *Manager) pendingTaskPath(task string) string {
	return pendingPrefix + task
}

func (m *Manager) runningTaskPath(task string) string {
	return runningPrefix + task
}

func (m *Manager) newTask(ts time.Time, task string) error {
	m.Lock()
	defer m.Unlock()

	if !m.isLeader {
		return errors.New("not leader")
	}

	pendingPath := m.pendingTaskPath(task)
	pendingResp, err := m.cli.Get(context.TODO(), pendingPath)
	if err != nil {
		return err
	}

	runningPath := m.runningTaskPath(task)
	runningResp, err := m.cli.Get(context.TODO(), runningPath)
	if err != nil {
		return err
	}

	// check this task was not existed before
	if pendingResp.Count != 0 || runningResp.Count != 0 {
		return errors.New("task exists")
	}

	_, err = m.cli.Put(context.TODO(), pendingPath, strconv.FormatInt(ts.Unix(), 10))

	if err != nil {
		return err
	}

	return m.allocatePendingTasks()
}

func (m *Manager) deleteTask(task string) error {
	m.Lock()
	defer m.Unlock()

	if !m.isLeader {
		return errors.New("not leader")
	}

	pendingPath := m.pendingTaskPath(task)
	pendingResp, err := m.cli.Delete(context.TODO(), pendingPath)
	if err != nil {
		return err
	}

	// delete a pending task
	if pendingResp.Deleted > 0 {
		return nil
	}

	runningPath := m.runningTaskPath(task)
	runningResp, err := m.cli.Get(context.TODO(), runningPath)
	if err != nil {
		return err
	}

	if runningResp.Count == 0 {
		return nil
	}

	// delete a running task
	metadata := string(runningResp.Kvs[0].Value)
	slice := strings.Split(metadata, "$$$")
	worker := slice[0]

	_, err = m.cli.Txn(context.TODO()).Then(
		clientv3.OpDelete(procPrefix+worker+"/"+task),
		clientv3.OpDelete(runningPath),
	).Commit()

	return err
}

func (m *Manager) newWorkerHandler() error {
	m.Lock()
	defer m.Unlock()

	if !m.isLeader {
		return errors.New("not leader")
	}

	return m.allocatePendingTasks()
}

func (m *Manager) delWorkerHandler(uuid string) error {
	m.Lock()
	defer m.Unlock()

	if !m.isLeader {
		return errors.New("not leader")
	}

	workerAddr := strings.TrimPrefix(uuid, workerPrefix)

	// get tasks offered to worker
	resp, _ := m.cli.Get(context.TODO(), procPrefix+workerAddr, clientv3.WithPrefix())
	for _, kv := range resp.Kvs {
		path := string(kv.Key)
		taskName := strings.TrimPrefix(path, procPrefix+workerAddr+"/")
		ts := string(kv.Value)

		_, err := m.cli.Txn(context.TODO()).
			Then(
				clientv3.OpDelete("/tasks/running/"+taskName),
				clientv3.OpPut("/tasks/pending/"+taskName, ts),
			).
			Commit()

		if err != nil {
			return err
		}
	}

	// we have move all running works in this worker to pending list,
	// at the we need to remove proc node
	_, err := m.cli.Delete(context.TODO(), procPrefix+workerAddr, clientv3.WithPrefix())
	if err != nil {
		return err
	}

	// re-allocate
	return m.allocatePendingTasks()
}
