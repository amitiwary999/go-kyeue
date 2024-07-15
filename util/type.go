package util

import (
	model "github.com/amitiwary999/go-kyeue/model"
)

type QueueStorgae interface {
	Save(string, []byte, string) error
	Read(int, string) ([]model.Message, error)
	ReadWithOffset(string, string) ([]model.Message, error)
	ReadWithOffsetTime(string, string) ([]model.Message, error)
}
