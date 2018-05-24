package AsyncPublisher

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
	ErrEmptyServer       = errors.New("empty servers")
	ErrConfirmChanClosed = errors.New("confirm channel is closed, can not publish now")
)

// DO NOT construct an instance manually, use NewAsyncPublisher() method instead.
type AsyncPublisher struct {
	updateLock         *sync.RWMutex
	servers            []string
	queueName          string
	exchangeName       string
	confirmChanSize    int
	confirmChanHealthy bool
	callback           rabbitUtil.PublishCallBack
	conn               *amqp.Connection
	ch                 *amqp.Channel
	confirmChan        chan amqp.Confirmation
}

// Example address: "amqp://guest:guest@127.0.0.1:5672".
// No need to call Destroy() method on the returning publisher manually, just consider it as RAII enabled.
// But, if GOGC=off you should call Destroy() on the returning publisher manually.
// -- maxAsync: The maximum number of unconfirmed messages.
// -- cb: The selfdefined function to process the confirmation.
func NewAsyncPublisher(addrs []string, queue, exchange string, maxAsync int, cb rabbitUtil.PublishCallBack) (rabbitUtil.MQPublisher, error) {
	var err error
	publisher := &AsyncPublisher{
		updateLock:      &sync.RWMutex{},
		servers:         addrs,
		queueName:       queue,
		exchangeName:    exchange,
		confirmChanSize: maxAsync,
		callback:        cb,
	}
	runtime.SetFinalizer(publisher, Destroy)
	err = publisher.reconnect()
	if err != nil {
		return nil, err
	}
	return publisher, nil
}

// Publish a message to the broker, don't wait for confirmation.
// The confirmation is handled by the callback function you passed to NewAsyncPublisher().
// Return any error if fail.
func (this *AsyncPublisher) Publish(message []byte) error {
	this.updateLock.RLock()
	defer this.updateLock.RUnlock()
	if !this.confirmChanHealthy {
		go this.reconnect()
		return ErrConfirmChanClosed
	}
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
	return nil
}

// Close all the IO resource assosiate with the publisher.
// It is not goroutine safe, try not calling it manually.
// TODO: make it better plz.
func (this *AsyncPublisher) Destroy() {
	if this.confirmChan != nil {
		close(this.confirmChan)
		this.confirmChan = nil
	}
	if this.ch != nil {
		this.ch.Close()
		this.ch = nil
	}
	if this.conn != nil {
		this.conn.Close()
		this.conn = nil
	}
	this.confirmChanHealthy = false
}
func (this *AsyncPublisher) reconnect() error {
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
	this.confirmChan = this.ch.NotifyPublish(make(chan amqp.Confirmation, this.confirmChanSize))
	this.ch.Confirm(false)
	this.confirmChanHealthy = true
	go func() {
		for {
			this.updateLock.RLock()
			confirmed, ok := <-this.confirmChan
			this.updateLock.RUnlock()
			if !ok {
				this.updateLock.Lock()
				defer this.updateLock.Unlock()
				this.confirmChanHealthy = false
				return
			}
			if this.callback != nil {
				go this.callback(confirmed)
			}
		}
	}()
	return nil
}

// general server selector,
// choose server randomly.
func (this *AsyncPublisher) selectServer() (chosen string, err error) {
	length := len(this.servers)
	if length <= 0 {
		return "", ErrEmptyServer
	}
	rand.Seed(time.Now().UnixNano())
	chosen = this.servers[rand.Int()%length]
	return chosen, nil
}
