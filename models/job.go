package models

import (
	"github.com/google/uuid"
)

type Priority int

const (
	Critical Priority = iota
	High
	Medium
	Low
)

type Job struct {
	ID       uuid.UUID
	Priority Priority
	DoJob    func() error
}
