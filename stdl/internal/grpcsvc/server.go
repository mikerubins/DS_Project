// Package grpcsvc implements the KVStore gRPC service.
//
// STEP 4 — In Part I every RPC returns a DUMMY response: the goal is only to
// prove that the .proto compiles and that two processes can talk over gRPC.
// In Part II this type will apply operations to the state machine, and in
// Part III it will submit them to the replicated log.
package grpcsvc

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	stdlv1 "github.com/mikerubins/DS_Project/stdl/gen/stdl/v1"
)

// Server implements stdlv1.STDLServer.
//
// Embedding UnimplementedSTDLServer makes every RPC not written yet return
// codes.Unimplemented (HTTP 501 after translation): when you add RPCs for your
// own service, the project keeps compiling while you work through them.
type Server struct {
	stdlv1.UnimplementedSTDListServer

	NodeID string // shown in dummy values, so you can see WHICH server answered
}

func New(nodeID string) *Server { return &Server{NodeID: nodeID} }

// dummyTasks is the pretend content of the store. The task "missing" never
// exists, so you can test the 404 path end to end.
var dummyTasks = []string{"alpha", "beta", "gamma"}

// The dummy store pretends that every existing task is at this version.
const dummyVersion = 1

// ---------------------------------------------------------------------------
// The example service (a key-value store). Replace it with your own service.
// ---------------------------------------------------------------------------

// Read is server streaming: one ReadResponse per entry, in sort order.
func (s *Server) Read(req *stdlv1.ReadRequest, stream stdlv1.STDList_ReadServer) error {
	var tasks []string = nil
	var tasksTemp []string = nil
	switch sel := req.GetSelect().(type) {
		case *stdlv1.ReadRequest_ReadOneRequest:
			tasks = []string{sel.ReadOneRequest.GetTask()}
			if sel.ReadOneRequest.GetTask() == "missing" || sel.ReadOneRequest.GetTask() == "" {
				return status.Errorf(codes.NotFound, "task %q not found", sel.ReadOneRequest.GetTask())
			}
			return nil
		case *stdlv1.ReadRequest_ReadMultipleRequests: // several tasks: missing ones are skipped
			// Variables that participate in filtering the tasks (intersection of all filters)
			var tasksReq = sel.ReadMultipleRequests.GetTasks(); // task filter
			var participants = sel.ReadMultipleRequests.GetParticipants(); // task participants filter
			var priorities = sel.ReadMultipleRequests.GetPriorities(); // task priorities filter
			var byCompleted = sel.ReadMultipleRequests.GetCompleted(); // task completed filter
			var byOpen = sel.ReadMultipleRequests.GetOpen(); // task open filter
			var prefix = sel.ReadMultipleRequests.GetPrefix(); // task prefix filter
			var rangeTask = sel.ReadMultipleRequests.GetRange(); // task range filter [from, to)
			var sort = sel.ReadMultipleRequests.GetSort(); // task sort order
			var limit = sel.ReadMultipleRequests.GetLimit();  // 0 = no limit
			var tasks_only = sel.ReadMultipleRequests.GetTasksOnly();  // entries carry only the task
			// If no filters presented, return all tasks
			if len(tasks) == 0 && len(participants) == 0 && len(priorities) == 0 && len(completed) == 0 && prefix == "" && rangeTask == nil {
				tasks = append(tasks, dummyTasks...)
			}
			else {
				// Task filtering
				if len(tasksReq) > 0 {
					for _, k := range tasksReq {
						if k != "missing" && k != "" {
							tasks = append(tasks, k)
						}
					}
				}
				else {
					tasks = append(tasks, dummyTasks...)
				}
				// Prefix filtering
				if prefix != "" {
					for _, k := range tasks {
						if strings.HasPrefix(k, prefix) {
							tasksTemp = append(tasksTemp, k)
						}
					}
					tasks = tasksTemp
					tasksTemp = nil
				}
				// Completed tasks filtering
				if byCompleted == true {
					for _, k := range tasks {
						if k.GetCompleted() == true {
							tasksTemp = append(tasksTemp, k)
						}
					}
					tasks = tasksTemp
					tasksTemp = nil
				}
				// Open tasks filtering
				if byOpen == true {
					for _, k := range tasks {
						if k.GetCompleted() == false {
							tasksTemp = append(tasksTemp, k)
						}
					}
					tasks = tasksTemp
					tasksTemp = nil
				}
				// Range filtering
				if rangeTask != nil { // [from, to); an empty bound is open
					for _, k := range tasks {
						if k >= rangeTask.GetFrom() && (rangeTask.GetTo() == "" || k < rangeTask.GetTo()) {
							keys = append(keys, k)
						}
					}
				}
					tasks = tasksTemp
					tasksTemp = nil
				}
				// Participants filtering
				if len(participants) > 0 {
					for _, k := range tasks {
						for _, p := range participants {
							if k.GetParticipants() == p {
								tasksTemp = append(tasksTemp, k)
								break
							}
						}
					}
					tasks = tasksTemp
					tasksTemp = nil
				}
				// Priorities filtering
				if len(priorities) > 0 {
					for _, k := range tasks {
						for _, p := range priorities {
							if k.GetPriority() == p {
								tasksTemp = append(tasksTemp, k)
								break
							}
						}
					}
					tasks = tasksTemp
					tasksTemp = nil
				}
			}
	case nil:
		return status.Errorf(codes.InvalidArgument, "no selector specified for read")
	}

	// Sorting by task only
	sort.Strings(tasks)
	if req.GetSort().GetDescending() {
		for i, j := 0, len(tasks)-1; i < j; i, j = i+1, j-1 {
			tasks[i], tasks[j] = tasks[j], tasks[i]
		}
	}
	if n := int(req.GetLimit()); n > 0 && n < len(tasks) {
		tasks = tasks[:n]
	}

	for _, k := range tasks {
		e := &stdlv1.Entry{Task: k, Version: dummyVersion}
		if !req.GetTasksOnly() {
			e.Name = fmt.Sprintf("dummy from %s (%s read)", s.NodeID, req.GetConsistency())
		}
		if err := stream.Send(&stdlv1.ReadResponse{Entry: e}); err != nil {
			return err
		}
	}
	return nil
}

// Update is client streaming: the client sends one UpdateRequest per task and
// closes the stream; the server answers once, for the whole batch.
func (s *Server) Update(stream stdlv1.STDList_UpdateServer) error {
	reqs, err := recvAll[stdlv1.UpdateRequest](stream.Recv)
	if err != nil {
		return err
	}
	if len(reqs) == 0 {
		return status.Error(codes.InvalidArgument, "no updates")
	}
	// Check the whole batch before "applying" anything: all or nothing.
	for _, r := range reqs {
		if r.IfVersion != nil && r.GetIfVersion() != dummyVersion {
			return status.Errorf(codes.FailedPrecondition,
				"key %q is at version %d, not %d", r.GetKey(), dummyVersion, r.GetIfVersion())
		}
	}
	out := &stdlv1.UpdateResponse{}
	for _, r := range reqs { // nothing is stored: compute the result and echo it
		var value string
		switch op := r.GetOp().(type) {
		case *stdlv1.UpdateRequest_Set:
			value = op.Set
		case *stdlv1.UpdateRequest_Append:
			value = "dummy" + op.Append // pretend the stored value was "dummy"
		case *stdlv1.UpdateRequest_Increment:
			value = fmt.Sprint(41 + op.Increment) // pretend it was 41
		default:
			return status.Errorf(codes.InvalidArgument, "key %q: no operation", r.GetKey())
		}
		out.Entries = append(out.Entries, &stdlv1.Entry{Key: r.GetKey(), Value: value, Version: dummyVersion + 1})
		out.Created = append(out.Created, r.GetKey() == "missing") // only "missing" is new
	}
	return stream.SendAndClose(out)
}

// Delete is client streaming as well: one DeleteRequest per key.
func (s *Server) Delete(stream stdlv1.STDList_DeleteServer) error {
	reqs, err := recvAll[stdlv1.DeleteRequest](stream.Recv)
	if err != nil {
		return err
	}
	var deleted int32
	for _, r := range reqs {
		if r.IfVersion != nil && r.GetIfVersion() != dummyVersion {
			return status.Errorf(codes.FailedPrecondition,
				"key %q is at version %d, not %d", r.GetKey(), dummyVersion, r.GetIfVersion())
		}
		if r.GetKey() != "missing" {
			deleted++
		}
	}
	return stream.SendAndClose(&stdlv1.DeleteResponse{Deleted: deleted})
}

// recvAll reads a client stream until the client closes it (io.EOF).
func recvAll[T any](recv func() (*T, error)) ([]*T, error) {
	var all []*T
	for {
		m, err := recv()
		if errors.Is(err, io.EOF) {
			return all, nil
		}
		if err != nil {
			return nil, err
		}
		all = append(all, m)
	}
}
