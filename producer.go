package easykafka

import (
	"time"

	"github.com/IBM/sarama"
)

type AckFunc func(*sarama.ProducerMessage)
type ErrFunc func(*sarama.ProducerError)

// Producer ...
type Producer struct {
	ap      sarama.AsyncProducer
	codec   Codec
	ackFunc AckFunc
	errFunc ErrFunc
	closed  bool
}

// NewProducer ...
func NewProducer(hosts []string, options ...Option) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	cfg.Producer.Timeout = time.Microsecond * 100
	for _, o := range options {
		o(cfg)
	}

	p, err := sarama.NewAsyncProducer(hosts, cfg)
	if err != nil {
		return nil, err
	}

	producer := &Producer{
		ap:    p,
		codec: DefaultCodec,
	}
	if cfg.Producer.Return.Errors || cfg.Producer.Return.Successes {
		go producer.run()
	}
	return producer, nil
}

// GetAsyncProducer ...
func (p *Producer) GetAsyncProducer() sarama.AsyncProducer {
	return p.ap
}

// SetError ...
func (p *Producer) SetError(errFunc ErrFunc) {
	p.errFunc = errFunc
}

// SetSuccess ...
func (p *Producer) SetSuccess(ackFunc AckFunc) {
	p.ackFunc = ackFunc
}

// Run ...
func (p *Producer) run() {
	success := p.ap.Successes()
	errors := p.ap.Errors()

	for {
		select {
		case ack, ok := <-success:
			if !ok {
				return
			}
			if p.ackFunc != nil {
				p.ackFunc(ack)
			}
		case err, ok := <-errors:
			if !ok {
				return
			}
			if p.errFunc != nil {
				p.errFunc(err)
			}
		}
	}
}

// SetCodec ...
func (p *Producer) SetCodec(codec Codec) {
	p.codec = codec
}

// Publish ...
func (p *Producer) Publish(topic string, data interface{}) error {
	if p.closed {
		return ErrAlreadyClosed
	}

	encodeData, err := p.codec.Marshal(data)
	if err != nil {
		return err
	}

	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Value = sarama.ByteEncoder(encodeData)

	p.ap.Input() <- msg
	return nil
}

// PublishString ...
func (p *Producer) PublishString(topic, message string) error {
	if p.closed {
		return ErrAlreadyClosed
	}

	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Value = sarama.StringEncoder(message)

	p.ap.Input() <- msg
	return nil
}

// PublishRawMsg ...
func (p *Producer) PublishRawMsg(msg *sarama.ProducerMessage) error {
	if p.closed {
		return ErrAlreadyClosed
	}

	p.ap.Input() <- msg
	return nil
}

// Close ...
func (p *Producer) Close() error {
	p.closed = true
	return p.ap.Close()
}
