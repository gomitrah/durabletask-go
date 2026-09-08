// Package tests_workflow exercises the public github.com/dapr/durabletask-go/workflow
// package end to end over a real gRPC connection, the same way an external consumer of
// that package would use it.
package tests_workflow

import (
	"context"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
	"github.com/dapr/durabletask-go/workflow"
)

var (
	ctx            = context.Background()
	workflowClient *workflow.Client
)

// TestMain sets up a gRPC server and a workflow.Client connected to it, mirroring
// tests/grpc/grpc_test.go but driving everything through the public workflow package
// instead of the lower-level task/client packages.
func TestMain(m *testing.M) {
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), backend.DefaultLogger())
	logger := backend.DefaultLogger()
	grpcServer := grpc.NewServer()
	grpcExecutor, registerFn := backend.NewGrpcExecutor(be, logger)
	registerFn(grpcServer)
	workflowWorker := backend.NewWorkflowWorker(backend.WorkflowWorkerOptions{
		Backend:  be,
		Executor: grpcExecutor,
		Logger:   logger,
		AppID:    "testapp",
	})
	activityWorker := backend.NewActivityTaskWorker(be, grpcExecutor, logger)
	taskHubWorker := backend.NewTaskHubWorker(be, workflowWorker, activityWorker, logger)
	if err := taskHubWorker.Start(ctx); err != nil {
		log.Fatalf("failed to start worker: %v", err)
	}

	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	time.Sleep(1 * time.Second)

	conn, err := grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to gRPC server: %v", err)
	}
	defer conn.Close()
	workflowClient = workflow.NewClient(conn)

	exitCode := m.Run()

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := grpcExecutor.Shutdown(timeoutCtx); err != nil {
		log.Fatalf("failed to shutdown grpc executor: %v", err)
	}

	timeoutCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := taskHubWorker.Shutdown(timeoutCtx); err != nil {
		log.Fatalf("failed to shutdown worker: %v", err)
	}
	grpcServer.Stop()
	os.Exit(exitCode)
}

func startWorker(t *testing.T, r *workflow.Registry) context.CancelFunc {
	cancelCtx, cancel := context.WithCancel(ctx)
	require.NoError(t, workflowClient.StartWorker(cancelCtx, r))
	return cancel
}

// Test_Workflow_Select_ExternalEventRace demonstrates the scenario from
// https://github.com/dapr/dapr/issues/10447 using only the public workflow package:
// a workflow that must respond to whichever of several named external events arrives
// first, using workflow.WorkflowContext.Select instead of polling.
func Test_Workflow_Select_ExternalEventRace(t *testing.T) {
	r := workflow.NewRegistry()
	require.NoError(t, r.AddWorkflowN("SelectRaceWorkflow", func(ctx *workflow.WorkflowContext) (any, error) {
		approve := ctx.WaitForExternalEvent("Approve", -1)
		reject := ctx.WaitForExternalEvent("Reject", -1)
		abort := ctx.WaitForExternalEvent("Abort", -1)

		winner, err := ctx.Select(approve, reject, abort)
		if err != nil {
			return nil, err
		}

		tasks := []workflow.Task{approve, reject, abort}
		names := []string{"Approve", "Reject", "Abort"}

		var v string
		if err := tasks[winner].Await(&v); err != nil {
			return nil, err
		}
		return names[winner] + ":" + v, nil
	}))

	cancel := startWorker(t, r)
	defer cancel()

	id, err := workflowClient.ScheduleWorkflow(ctx, "SelectRaceWorkflow")
	require.NoError(t, err)

	_, err = workflowClient.WaitForWorkflowStart(ctx, id)
	require.NoError(t, err)

	// Only raise the event that should win the race; the workflow must not need
	// the other two to ever be raised.
	require.NoError(t, workflowClient.RaiseEvent(ctx, id, "Reject", workflow.WithEventPayload("nope")))

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 30*time.Second)
	defer cancelTimeout()
	metadata, err := workflowClient.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"Reject:nope"`, metadata.Output.Value)
}
