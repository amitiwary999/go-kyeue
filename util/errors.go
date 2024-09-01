package util

import (
	"errors"
	"slices"
)

const (
	undefinedTableErrorCode  = "42P01"
	undefinedColumnErrorCode = "42703"
)

var pgNonRecoveredError = []string{undefinedColumnErrorCode, undefinedTableErrorCode}

type pgError interface {
	SQLState() string
}

func IsPgNonRecoveredError(err error) bool {
	var pgErr pgError
	if ok := errors.As(err, &pgErr); !ok {
		return false
	}
	return slices.Contains(pgNonRecoveredError, pgErr.SQLState())
}
