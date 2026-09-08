package command

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ParseExtend accepts only the documented whole-hour lease-extension grammar.
// A zero duration denotes the bare command, whose configured increment is
// resolved by lifecycle.Manager.
func ParseExtend(text string) (time.Duration, error) {
	words := strings.Fields(text)
	if len(words) == 1 && words[0] == "extend" {
		return 0, nil
	}
	if len(words) != 2 || words[0] != "extend" {
		return 0, fmt.Errorf("extend accepts an optional whole number of hours from 1 through 24")
	}
	value := strings.TrimSuffix(words[1], "h")
	if value == "" {
		return 0, fmt.Errorf("extend accepts an optional whole number of hours from 1 through 24")
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return 0, fmt.Errorf("extend accepts an optional whole number of hours from 1 through 24")
		}
	}
	hours, err := strconv.Atoi(value)
	if err != nil || hours < 1 || hours > 24 {
		return 0, fmt.Errorf("extend accepts an optional whole number of hours from 1 through 24")
	}
	return time.Duration(hours) * time.Hour, nil
}
