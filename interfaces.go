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

type VersionInfo struct{ value interface{} }

func (this *VersionInfo) SetVersion(value interface{}) { this.value = value }
func (this *VersionInfo) Version() interface{}         { return this.value }
func (this *VersionInfo) Reset()                       { this.value = nil }
