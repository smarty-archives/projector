package transform

import (
	"sync"
	"time"
)

type multiTransformer struct {
	transformers []Transformer
	waiter       sync.WaitGroup
}

func newMultiTransformer(transformers []Transformer) *multiTransformer {
	return &multiTransformer{transformers: transformers}
}

func (this *multiTransformer) Transform(now time.Time, messages []interface{}) {
	count := len(this.transformers)
	this.waiter.Add(count)

	for i := 0; i < count; i++ {
		go this.transform(i, now, messages) // this for loop is safe to execute because it evaluates "i" before "go"
	}

	this.waiter.Wait()
}
func (this *multiTransformer) transform(index int, now time.Time, messages []interface{}) {
	this.transformers[index].Transform(now, messages)
	this.waiter.Done()
}
