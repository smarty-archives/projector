package projector

import "time"

type Document interface {
	Lapse(now time.Time) (next Document)
	Apply(message interface{}) bool
	Path() string

	Reset()
	SetVersion(interface{})
	Version() interface{}
}

type Version struct{ value interface{} }

func (this *Version) SetVersion(value interface{}) { this.value = value }
func (this *Version) Version() interface{}         { return this.value }
func (this *Version) Reset()                       { this.value = nil }
