package SyncPublisher

import (
	"errors"
	"math/rand"
	"runtime"
	"sync"
	"time"
	"github.com/leeming87v5/rabbitUtil"

	"github.com/streadway/amqp"
)

var (
	ErrConnectClose = errors.New("connection has been closed")
	ErrPublish      = errors.New("an error has occurred while publishing")
	ErrEmptyServer  = errors.New("empty servers")
)

// DO NOT construct an instance manually, use NewSyncPublisher() method instead.
type SyncPublisher struct {
	updateLock   *sync.RWMutex
	servers      []string
	queueName    string
	exchangeName string
	conn         *amqp.Connection
	ch           *amqp.Channel
	confirmChan  chan amqp.Confirmation
}

// Example address: "amqp://guest:guest@127.0.0.1:5672".
// No need to call Destroy() method on the returning publisher manually, just consider it as RAII enabled.
// But, if GOGC=off you should call Destroy() on the returning publisher manually.
func NewSyncPublisher(addrs []string, queue string, exchange string) (rabbitUtil.MQPublisher, error) {
	var err error
	publisher := &SyncPublisher{
		updateLock:   &sync.RWMutex{},
		servers:      addrs,
		queueName:    queue,
		exchangeName: exchange,
	}
	runtime.SetFinalizer(publisher, Destroy)
	err = publisher.reconnect()
	if err != nil {
		return nil, err
	}
	return publisher, nil
}

// Publish a message to the broker, and wait for confirmation.
// Return any error if fail.
func (this *SyncPublisher) Publish(message []byte) error {
	this.updateLock.RLock()
	defer this.updateLock.RUnlock()
	err := this.ch.Publish(
		this.exchangeName,
		this.queueName,
		false,
		false,
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: 2,
			Body:         message,
		})
	if err != nil {
		go this.reconnect()
		return err
	}
	confirmed, ok := <-this.confirmChan
	if !ok {
		return ErrConnectClose
	}
	if !confirmed.Ack {
		return ErrPublish
	}
	return nil
}

// Close all the IO resource assosiate with the publisher.
// It is not goroutine safe, try not calling it manually.
// TODO: make it better plz.
func (this *SyncPublisher) Destroy() {
	if this.confirmChan != nil {
		close(this.confirmChan)
		// this.confirmChan = nil
	}
	if this.ch != nil {
		this.ch.Close()
		// this.ch = nil
	}
	if this.conn != nil {
		this.conn.Close()
		// this.conn = nil
	}
}
func (this *SyncPublisher) reconnect() error {
	var err error
	this.updateLock.Lock()
	defer this.updateLock.Unlock()
	this.Destroy()
	server, err := this.selectServer()
	if err != nil {
		return err
	}
	this.conn, err = amqp.Dial(server)
	if err != nil {
		return err
	}
	this.ch, err = this.conn.Channel()
	if err != nil {
		return err
	}
	this.confirmChan = this.ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	this.ch.Confirm(false)
	return nil
}

// general server selector,
// choose server randomly.
func (this *SyncPublisher) selectServer() (chosen string, err error) {
	length := len(this.servers)
	if length <= 0 {
		return "", ErrEmptyServer
	}
	rand.Seed(time.Now().UnixNano())
	chosen = this.servers[rand.Int()%length]
	return chosen, nil
}
