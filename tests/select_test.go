package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/task"
)

// Test_Select_ExternalEventRace exercises the scenario from
// https://github.com/dapr/dapr/issues/10447: waiting on whichever of several
// named external events arrives first, without polling.
func Test_Select_ExternalEventRace(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectRaceWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		approve := ctx.WaitForSingleEvent("Approve", -1)
		reject := ctx.WaitForSingleEvent("Reject", -1)
		abort := ctx.WaitForSingleEvent("Abort", -1)

		winner, err := ctx.Select(approve, reject, abort)
		if err != nil {
			return nil, err
		}

		switch winner {
		case 0:
			var v string
			if err := approve.Await(&v); err != nil {
				return nil, err
			}
			return "Approve:" + v, nil
		case 1:
			var v string
			if err := reject.Await(&v); err != nil {
				return nil, err
			}
			return "Reject:" + v, nil
		default:
			var v string
			if err := abort.Await(&v); err != nil {
				return nil, err
			}
			return "Abort:" + v, nil
		}
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectRaceWorkflow")
	require.NoError(t, err)

	_, err = client.WaitForWorkflowStart(ctx, id)
	require.NoError(t, err)

	// Only raise the event that should win the race; the workflow must not
	// need the other two to ever be raised.
	require.NoError(t, client.RaiseEvent(ctx, id, "Reject", api.WithEventPayload("nope")))

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"Reject:nope"`, metadata.Output.Value)
}

// Test_Select_LoopOverRemaining models a workflow that must observe every one
// of several events, in whatever order they arrive, by repeatedly Selecting
// over the tasks that have not yet completed.
func Test_Select_LoopOverRemaining(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectLoopWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		pending := []task.Task{
			ctx.WaitForSingleEvent("First", -1),
			ctx.WaitForSingleEvent("Second", -1),
			ctx.WaitForSingleEvent("Third", -1),
		}
		var order []string
		for len(pending) > 0 {
			winner, err := ctx.Select(pending...)
			if err != nil {
				return nil, err
			}
			var v string
			if err := pending[winner].Await(&v); err != nil {
				return nil, err
			}
			order = append(order, v)
			pending = append(pending[:winner], pending[winner+1:]...)
		}
		return order, nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectLoopWorkflow")
	require.NoError(t, err)
	_, err = client.WaitForWorkflowStart(ctx, id)
	require.NoError(t, err)

	// Raise out of declaration order to prove Select isn't just returning
	// index 0 every time.
	require.NoError(t, client.RaiseEvent(ctx, id, "Third", api.WithEventPayload("c")))
	require.NoError(t, client.RaiseEvent(ctx, id, "First", api.WithEventPayload("a")))
	require.NoError(t, client.RaiseEvent(ctx, id, "Second", api.WithEventPayload("b")))

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `["c","a","b"]`, metadata.Output.Value)
}

// Test_Select_AlreadyCompletedWins verifies that a task which is already
// complete by the time Select is called (e.g. a timer created earlier in the
// same execution that has since fired) is picked immediately, without
// needing to process any further history.
func Test_Select_AlreadyCompletedWins(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectAlreadyCompletedWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		immediate := ctx.CreateTimer(0)
		if err := immediate.Await(nil); err != nil {
			return nil, err
		}

		neverFires := ctx.WaitForSingleEvent("NeverSent", -1)

		winner, err := ctx.Select(neverFires, immediate)
		if err != nil {
			return nil, err
		}
		return winner, nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectAlreadyCompletedWorkflow")
	require.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `1`, metadata.Output.Value)
}

// Test_Select_NoTasks verifies that calling Select with no tasks reports an
// error instead of panicking or blocking forever.
func Test_Select_NoTasks(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectNoTasksWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		_, err := ctx.Select()
		if err == nil {
			return nil, nil
		}
		return err.Error(), nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectNoTasksWorkflow")
	require.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"Select requires at least one task"`, metadata.Output.Value)
}

// Test_Select_RetryWrappedTaskRejected verifies that a task returned by
// CallActivity with a retry policy -- whose completion, including any
// retries, is only ever driven by calling Await -- is rejected by Select
// rather than silently never winning or racing incorrectly.
func Test_Select_RetryWrappedTaskRejected(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectRetryWrappedWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		retried := ctx.CallActivity("FailActivity", task.WithActivityRetryPolicy(&task.RetryPolicy{
			MaxAttempts:          2,
			InitialRetryInterval: 10 * time.Millisecond,
		}))
		plain := ctx.CreateTimer(1 * time.Hour)

		_, err := ctx.Select(retried, plain)
		if err == nil {
			return "no error", nil
		}
		return err.Error(), nil
	})
	r.AddActivityN("FailActivity", func(ctx task.ActivityContext) (any, error) {
		return nil, errors.New("activity should not have been invoked")
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectRetryWrappedWorkflow")
	require.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Contains(t, metadata.Output.Value, "task does not support Select")
}

