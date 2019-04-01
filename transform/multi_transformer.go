package transform

import (
	"sync"
	"time"
)

type MultiTransformer struct {
	transformers []Transformer
	waiter       sync.WaitGroup
}

func newMultiTransformer(transformers []Transformer) *MultiTransformer {
	return &MultiTransformer{transformers: transformers}
}

func (this *MultiTransformer) Transform(now time.Time, messages []interface{}) {
	count := len(this.transformers)
	this.waiter.Add(count)

	for i := 0; i < count; i++ {
		go this.transform(i, now, messages) // this for loop is safe to execute because it evaluates "i" before "go"
	}

	this.waiter.Wait()
}
func (this *MultiTransformer) transform(index int, now time.Time, messages []interface{}) {
	this.transformers[index].Transform(now, messages)
	this.waiter.Done()
}
