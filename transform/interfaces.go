package transform

import "time"

type Transformer interface {
	Transform(time.Time, []interface{})
}
