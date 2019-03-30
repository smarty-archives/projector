package projector

import "time"

type Document interface {
	Lapse(now time.Time) (next Document)
	Apply(message interface{}) bool
	Path() string
}
