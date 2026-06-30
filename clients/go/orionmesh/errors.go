package orionmesh

import "fmt"

type ResourceNotFoundError struct {
	Kind string
	Name string
}

func (e *ResourceNotFoundError) Error() string {
	return fmt.Sprintf("%s/%s not found", e.Kind, e.Name)
}

type ApplyFailedError struct {
	Status int
	Detail string
}

func (e *ApplyFailedError) Error() string {
	return fmt.Sprintf("apply failed (%d): %s", e.Status, e.Detail)
}

type DispatchFailedError struct {
	Detail string
}

func (e *DispatchFailedError) Error() string { return e.Detail }

type QueueNotFoundError struct {
	Name string
}

func (e *QueueNotFoundError) Error() string {
	return fmt.Sprintf("queue %s not declared — apply a Queue resource first", e.Name)
}
