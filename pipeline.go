package pipeline

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// SourceFn is a function to produce work items.
// It should call put(item) to add an item into the pipeline for processing.
type SourceFn[T any] func(ctx context.Context, put func(T) error) error

// StageFn is a function process a work item.
// It takes an item as an input an return a new one after processing.
// The last stage is a sink stage and its output will be ignored.
type StageFn[T any] func(ctx context.Context, item T) (T, error)

type Source[T any] func(context.Context, *errgroup.Group) <-chan T
type Stage[T any] func(context.Context, *errgroup.Group, <-chan T) <-chan T

// NewSource creates a new source stage to procude work items.
// They then be processed by each stage.
func NewSource[T any](sourceFn SourceFn[T]) Source[T] {
	return func(ctx context.Context, eg *errgroup.Group) <-chan T {
		out := make(chan T)
		eg.Go(func() error {
			putFn := func(data T) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case out <- data:
					return nil
				}
			}
			defer close(out)
			return sourceFn(ctx, putFn)
		})
		return out
	}
}

// NewStage creates a new stage to process work items.
func NewStage[T any](fn StageFn[T]) Stage[T] {
	return func(ctx context.Context, eg *errgroup.Group, inCh <-chan T) <-chan T {
		outCh := make(chan T)
		eg.Go(func() error {
			defer close(outCh)
			for in := range inCh {
				out, err := fn(ctx, in)
				if err != nil {
					return err
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				case outCh <- out:
				}
			}
			return nil
		})
		return outCh
	}
}

// Do runs the pipeline specified by the source and stages.
// Source will produce work items and each stage will process them.
func Do[T any](ctx context.Context, source Source[T], stages ...Stage[T]) error {
	eg, ctx := errgroup.WithContext(ctx)

	next := source(ctx, eg)
	for _, stage := range stages {
		next = stage(ctx, eg, next)
	}

	// exhaust the channel to make sure no stage are blocked.
	for range next {
	}

	return eg.Wait()
}
