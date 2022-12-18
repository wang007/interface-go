// Copyright 2022 wang007
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package iamqp091

import "context"
import amqp "github.com/rabbitmq/amqp091-go"

type (
	Channel              = amqp.Channel
	Table                = amqp.Table
	Publishing           = amqp.Publishing
	Delivery             = amqp.Delivery
	DeferredConfirmation = amqp.DeferredConfirmation

	Queue        = amqp.Queue
	Return       = amqp.Return
	Confirmation = amqp.Confirmation
	Error        = amqp.Error

	URI = amqp.URI
)

type Publisher interface {
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg Publishing) error
	PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg Publishing) (*DeferredConfirmation, error)
}

// Handler accepts Context and Delivery and return error maybe only record info
type Handler func(context.Context, *Delivery) error

type ExtDelivery interface {
	Raw() *Delivery
	AutoAck() bool
	Queue() string

	RunWithContext(ctx context.Context) (context.Context, func(err error))
	RunOnScopeWithContext(ctx context.Context, handler Handler)
}

// InternalConsumer for internal to reduce create goroutine
type InternalConsumer interface {
	RawConsume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args Table) (<-chan Delivery, error)
	WrapDelivery(delivery *Delivery, queue string, autoAck bool) ExtDelivery
}

type Consumer interface {
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args Table) (<-chan ExtDelivery, error)
}

// ExtChannel is interface for enhance iamqp.Channel
// Drop Publish, PublishWithDeferredConfirm method
type ExtChannel interface {
	Publisher
	Consumer
	Ack(tag uint64, multiple bool) error
	Cancel(consumer string, noWait bool) error
	Close() error
	Confirm(noWait bool) error
	ExchangeBind(destination, key, source string, noWait bool, args Table) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args Table) error
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args Table) error
	ExchangeDelete(name string, ifUnused, noWait bool) error
	ExchangeUnbind(destination, key, source string, noWait bool, args Table) error
	Flow(active bool) error
	Get(queue string, autoAck bool) (msg ExtDelivery, ok bool, err error)
	GetNextPublishSeqNo() uint64
	IsClosed() bool
	Nack(tag uint64, multiple bool, requeue bool) error
	NotifyCancel(c chan string) chan string
	NotifyClose(c chan *Error) chan *Error
	NotifyConfirm(ack, nack chan uint64) (chan uint64, chan uint64)
	NotifyFlow(c chan bool) chan bool
	NotifyPublish(confirm chan Confirmation) chan Confirmation
	NotifyReturn(c chan Return) chan Return

	Qos(prefetchCount, prefetchSize int, global bool) error
	QueueBind(name, key, exchange string, noWait bool, args Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args Table) (Queue, error)
	QueueDeclarePassive(name string, durable, autoDelete, exclusive, noWait bool, args Table) (Queue, error)
	QueueDelete(name string, ifUnused, ifEmpty, noWait bool) (int, error)
	QueueInspect(name string) (Queue, error)
	QueuePurge(name string, noWait bool) (int, error)
	QueueUnbind(name, key, exchange string, args Table) error
	Reject(tag uint64, requeue bool) error
	Tx() error
	TxCommit() error
	TxRollback() error

	GetURI() (URI, bool)
}

var _ ExtChannel = (*ConcreteChannel)(nil)
var _ InternalConsumer = (*ConcreteChannel)(nil)

type ConcreteChannel struct {
	*amqp.Channel
	uri URI
	ok  bool
}

func NewChannel(c *amqp.Channel, url string) *ConcreteChannel {
	ok := true
	uri, err := amqp.ParseURI(url)
	if err != nil {
		ok = false
	}
	return &ConcreteChannel{
		Channel: c,
		uri:     uri,
		ok:      ok,
	}
}

func NewChannelWithURI(c *amqp.Channel, uri URI) *ConcreteChannel {
	return &ConcreteChannel{
		Channel: c,
		uri:     uri,
		ok:      true,
	}
}

func (r *ConcreteChannel) GetURI() (URI, bool) {
	return r.uri, r.ok
}

func (r *ConcreteChannel) RawConsume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args Table) (<-chan Delivery, error) {
	return r.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (r *ConcreteChannel) WrapDelivery(delivery *Delivery, queue string, autoAck bool) ExtDelivery {
	return NewDelivery(delivery, queue, autoAck)
}

func (r *ConcreteChannel) PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg Publishing) error {
	return r.Channel.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (r *ConcreteChannel) PublishWithDeferredConfirmWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg Publishing) (*DeferredConfirmation, error) {
	return r.Channel.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
}

func (r *ConcreteChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args Table) (<-chan ExtDelivery, error) {
	deliveries, err := r.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		return nil, err
	}
	extDeliveries := make(chan ExtDelivery)
	go func() {
		defer close(extDeliveries)
		for d := range deliveries {
			copyD := d
			extDeliveries <- NewDelivery(&copyD, queue, autoAck)
		}
	}()
	return extDeliveries, nil
}

func (r *ConcreteChannel) Get(queue string, autoAck bool) (msg ExtDelivery, ok bool, err error) {
	d, ok, err := r.Channel.Get(queue, autoAck)
	if !ok {
		return nil, false, err
	}
	copyD := d
	return NewDelivery(&copyD, queue, autoAck), ok, err
}

var _ ExtDelivery = (*ConcreteDelivery)(nil)

type ConcreteDelivery struct {
	raw     *Delivery
	queue   string
	autoAck bool
}

func NewDelivery(raw *Delivery, queue string, autoAck bool) *ConcreteDelivery {
	return &ConcreteDelivery{
		raw:     raw,
		autoAck: autoAck,
		queue:   queue,
	}
}

func (c *ConcreteDelivery) Raw() *Delivery {
	return c.raw
}

func (c *ConcreteDelivery) AutoAck() bool {
	return c.autoAck
}

func (c *ConcreteDelivery) Queue() string {
	return c.queue
}

func (c *ConcreteDelivery) RunWithContext(ctx context.Context) (context.Context, func(err error)) {
	return ctx, func(err error) {}
}

func (c *ConcreteDelivery) RunOnScopeWithContext(ctx context.Context, handler Handler) {
	ctx, end := c.RunWithContext(ctx)
	err := handler(ctx, c.raw)
	end(err)
}
