package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/bongnv/pipeline"
	"github.com/google/go-cmp/cmp"
)

func TestPipeline(t *testing.T) {
	testCases := map[string]struct {
		invoke         func() ([]string, error)
		expectedErr    string
		expectedResult []string
	}{
		"should run through multiple stages": {
			invoke: func() ([]string, error) {
				ctx := context.Background()
				results := []string{}
				err := pipeline.Do(
					ctx,
					pipeline.NewSource(func(ctx context.Context, put func(string) error) error {
						_ = put("pipe")
						return put("line")
					}),
					pipeline.NewStage(func(ctx context.Context, name string) (string, error) {
						return strings.ToUpper(name), nil
					}),
					pipeline.NewStage(func(ctx context.Context, name string) (string, error) {
						results = append(results, name)
						return "", nil
					}),
				)
				return results, err
			},
			expectedResult: []string{
				"PIPE",
				"LINE",
			},
			expectedErr: "<nil>",
		},
		"should propergate the error from the source": {
			invoke: func() ([]string, error) {
				ctx := context.Background()
				results := []string{}
				sinked := make(chan struct{})
				err := pipeline.Do(
					ctx,
					pipeline.NewSource(func(ctx context.Context, put func(string) error) error {
						_ = put("pipe")
						<-sinked
						return errors.New("source error")
					}),
					pipeline.NewStage(func(ctx context.Context, name string) (string, error) {
						return strings.ToUpper(name), nil
					}),
					pipeline.NewStage(func(ctx context.Context, name string) (string, error) {
						results = append(results, name)
						close(sinked)
						return "", nil
					}),
				)
				return results, err
			},
			expectedResult: []string{
				"PIPE",
			},
			expectedErr: "source error",
		},
		"should propergate the error from a stage": {
			invoke: func() ([]string, error) {
				ctx := context.Background()
				results := []string{}
				sinked := make(chan struct{})
				err := pipeline.Do(
					ctx,
					pipeline.NewSource(func(ctx context.Context, put func(string) error) error {
						_ = put("pipe")
						return put("line")
					}),
					pipeline.NewStage(func(ctx context.Context, name string) (string, error) {
						select {
						case <-sinked:
							return "", errors.New("stage error")
						default:
							return strings.ToUpper(name), nil
						}
					}),
					pipeline.NewStage(func(ctx context.Context, name string) (string, error) {
						results = append(results, name)
						close(sinked)
						return "", nil
					}),
				)
				return results, err
			},
			expectedResult: []string{
				"PIPE",
			},
			expectedErr: "stage error",
		},
		"should return an error when context is canceled": {
			invoke: func() ([]string, error) {
				ctx, cancel := context.WithCancel(context.Background())
				results := []string{}
				sinked := make(chan struct{})
				canceled := make(chan struct{})
				go func() {
					<-sinked
					cancel()
					close(canceled)
				}()
				err := pipeline.Do(
					ctx,
					pipeline.NewSource(func(ctx context.Context, put func(string) error) error {
						_ = put("pipe")
						<-canceled
						return put("line")
					}),
					pipeline.NewStage(func(ctx context.Context, name string) (string, error) {
						return strings.ToUpper(name), nil
					}),
					pipeline.NewStage(func(ctx context.Context, name string) (string, error) {
						results = append(results, name)
						close(sinked)
						return "", nil
					}),
				)
				return results, err
			},
			expectedResult: []string{
				"PIPE",
			},
			expectedErr: "context canceled",
		},
	}

	for name, tc := range testCases {
		tc := tc
		t.Run(name, func(t *testing.T) {
			result, err := tc.invoke()
			if diff := cmp.Diff(tc.expectedErr, fmt.Sprintf("%v", err)); diff != "" {
				t.Errorf("Pipeline produced a different error %s", diff)
			}
			if diff := cmp.Diff(tc.expectedResult, result); diff != "" {
				t.Errorf("Pipeline produced a different result %s", diff)
			}
		})
	}
}
