package main

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestWaitForCleanup(t *testing.T) {
	var group sync.WaitGroup
	if err := waitForCleanup(context.Canceled, &group); err != nil {
		t.Fatal(err)
	}
	if err := waitForCleanup(errors.New("stopped"), &group); err == nil {
		t.Fatal("missing transport error")
	}
}
