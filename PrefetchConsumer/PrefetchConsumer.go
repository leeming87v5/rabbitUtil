package PrefetchConsumer

import (
	// "github.com/neverlee/glog"
	"errors"
	"math/rand"
	"rabbitUtil"
	"runtime"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

// DO NOT construct an instance manually, use NewPrefetchConsumer() method instead.
type PrefetchConsumer struct {
	updateLock *sync.RWMutex
	servers    []string
	queueName  string
	conn       *amqp.Connection
	ch         *amqp.Channel
	delivery   <-chan amqp.Delivery
}

// Example address: "amqp://guest:guest@127.0.0.1:5672".
// No need to call Destroy() method on the returning consumer manually, just consider it as RAII enabled.
// But, if GOGC=off you should call Destroy() on the returning consumer manually.
func NewPrefetchConsumer(addrs []string, queue string) (rabbitUtil.MQConsumer, error) {
	var err error
	consumer := &PrefetchConsumer{
		updateLock: &sync.RWMutex{},
		servers:    addrs,
		queueName:  queue,
	}
	runtime.SetFinalizer(consumer, Destroy)
	err = consumer.reconnect()
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

var (
	ErrConnectClose = errors.New("connection has been closed")
	ErrEmptyServer  = errors.New("empty servers")
)

// Fetch one message from queue, then pass it to the callback function for processing.
// -- callback: The selfdefined function to process the message.
// -- args: Extra arguments passing to the callback function.
// Return ErrConnectClose when message channel has been closed, otherwise always return nil.
func (this *PrefetchConsumer) Consume(callback rabbitUtil.ConsumeCallBack, args ...interface{}) error {
	this.updateLock.RLock()
	defer this.updateLock.RUnlock()
	delivery, ok := <-this.delivery
	if !ok {
		return ErrConnectClose
	}
	err := callback(delivery.Body, args)
	if err != nil {
		errNack := delivery.Nack(false, true)
		if errNack != nil {
			go this.reconnect()
		}
	} else {
		errAck := delivery.Ack(false)
		if errAck != nil {
			go this.reconnect()
		}
	}
	return nil
}

// Close all the IO resource assosiate with the consumer.
// It is not goroutine safe, try not calling it manually.
// TODO: make it better plz.
func (this *PrefetchConsumer) Destroy() {
	/*
		if this.delivery != nil {
			close(this.delivery)
		}
	*/
	if this.ch != nil {
		this.ch.Close()
		this.ch = nil
	}
	if this.conn != nil {
		this.conn.Close()
		this.conn = nil
	}
}
func (this *PrefetchConsumer) reconnect() error {
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
	err = this.ch.Qos(30, 0, false)
	if err != nil {
		return err
	}
	this.delivery, err = this.ch.Consume(
		this.queueName, // queue name
		"",             // consumer name
		false,          // autoAck
		false,          // exclusive
		false,          // noLoacl
		false,          // noWait
		nil,            // args
	)
	if err != nil {
		return err
	}
	return nil
}

// general server selector,
// choose server randomly.
func (this *PrefetchConsumer) selectServer() (chosen string, err error) {
	length := len(this.servers)
	if length <= 0 {
		return "", ErrEmptyServer
	}
	rand.Seed(time.Now().UnixNano())
	chosen = this.servers[rand.Int()%length]
	return chosen, nil
}
