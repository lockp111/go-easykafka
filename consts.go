package easykafka

import (
	"errors"

	"github.com/IBM/sarama"
)

// errors defined
var (
	ErrAlreadyClosed = errors.New("producer already closed")
	ErrGroupNotFound = errors.New("group not found")
)

type Option func(*sarama.Config)
