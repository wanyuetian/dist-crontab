package common

import "errors"

var (
	ErrLockAlreadyRequired = errors.New("锁已被占用")
	ErrNoLocalIPFound = errors.New("没有找到网卡IP")
)