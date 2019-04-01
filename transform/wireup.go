package transform

import (
	"github.com/smartystreets/listeners"
	"github.com/smartystreets/messaging"
	"github.com/smartystreets/projector"
	"github.com/smartystreets/projector/persist"
)

func NewHandler(i <-chan messaging.Delivery, o chan<- interface{}, rw persist.ReadWriter, d ...projector.Document) listeners.Listener {
	return newHandler(i, o, newTransformer(rw, d...))
}
