package util

import (
	model "github.com/amitiwary999/go-kyeue/model"
)

type QueueStorgae interface {
	CreateChannel(string) error
	Save(string, []byte, string) error
	Read(int, string) ([]model.Message, error)
	ReadPrevMessageOnLoad(string, string, string) ([]model.Message, error)
}
