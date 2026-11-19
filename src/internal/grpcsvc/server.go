// Package grpcsvc implements the simple to-do list gRPC service.
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

type Server struct {
	stdlv1.UnimplementedSTDListServer
	NodeID string // Identifies the node in dummy/logging responses.
}

func New(nodeID string) *Server {
	return &Server{NodeID: nodeID}
}

// dummyTaskNames is the fixed task set returned by the Part I service.
var dummyTaskNames = []string{"alpha", "beta", "gamma"}

const dummyVersion int64 = 1

// dummyEntry returns the fixed EntryBody for a known task. It also acts as
// the existence check for Read, Update, and Delete operations.
func dummyEntry(task string) *stdlv1.Entry {
	switch task {
	case "alpha":
		return &stdlv1.Entry{
			Task: task,
			EntryBody: &stdlv1.EntryBody{
				Name: "Alpha", Description: "dummy alpha task",
				Participants: []string{"alice", "bob"}, Priority: 1,
				Completed: false, Version: dummyVersion,
			},
		}
	case "beta":
		return &stdlv1.Entry{
			Task: task,
			EntryBody: &stdlv1.EntryBody{
				Name: "Beta", Description: "dummy beta task",
				Participants: []string{"bob"}, Priority: 3,
				Completed: true, Version: dummyVersion,
			},
		}
	case "gamma":
		return &stdlv1.Entry{
			Task: task,
			EntryBody: &stdlv1.EntryBody{
				Name: "Gamma", Description: "dummy gamma task",
				Participants: []string{"carol"}, Priority: 5,
				Completed: false, Version: dummyVersion,
			},
		}
	default:
		return nil
	}
}

func dummyDescription(description, nodeID string) string {
	if description == "" {
		return fmt.Sprintf("dummy response from %s", nodeID)
	}
	return fmt.Sprintf("%s (dummy from %s)", description, nodeID)
}

func dummyEntryForNode(entry *stdlv1.Entry, nodeID string) *stdlv1.Entry {
	if entry != nil && entry.GetEntryBody() != nil {
		entry.EntryBody.Description = dummyDescription(entry.GetEntryBody().GetDescription(), nodeID)
	}
	return entry
}

func dummyEntryList(tasks []string, tasksOnly bool, nodeID string) []*stdlv1.Entry {
	entries := make([]*stdlv1.Entry, 0, len(tasks))
	for _, task := range tasks {
		entry := dummyEntry(task)
		if entry == nil {
			continue
		}
		if tasksOnly {
			entry = &stdlv1.Entry{Task: task}
		} else {
			entry = dummyEntryForNode(entry, nodeID)
		}
		entries = append(entries, entry)
	}
	return entries
}

// Read is a server-streaming RPC. For Part I it filters and sorts dummy entries.
func (s *Server) Read(req *stdlv1.ReadRequest, stream stdlv1.STDList_ReadServer) error {
	if req == nil {
		return status.Error(codes.InvalidArgument, "read request is required")
	}

	var tasks []string
	switch sel := req.GetSelect().(type) {
	case *stdlv1.ReadRequest_OneRequest:
		if sel.OneRequest == nil {
			return status.Error(codes.InvalidArgument, "oneRequest is required")
		}
		task := sel.OneRequest.GetTask()
		if task == "" {
			return status.Error(codes.InvalidArgument, "task is required")
		}
		entry := dummyEntry(task)
		if entry == nil {
			return status.Errorf(codes.NotFound, "task %q not found", task)
		}
		entry = dummyEntryForNode(entry, s.NodeID)
		if err := stream.Send(&stdlv1.ReadResponse{Entry: entry}); err != nil {
			return err
			}
			return nil

	case *stdlv1.ReadRequest_MultipleRequests:
		if sel.MultipleRequests == nil {
			return status.Error(codes.InvalidArgument, "multipleRequests is required")
		}
		filter := sel.MultipleRequests

		if filter.GetByCompleted() && filter.GetByOpen() {
			return status.Error(codes.InvalidArgument, "byCompleted and byOpen cannot both be true")
		}
		if filter.GetLimit() < 0 {
			return status.Error(codes.InvalidArgument, "limit cannot be negative")
		}
		for _, priority := range filter.GetPriorities() {
			if priority < 1 || priority > 5 {
				return status.Errorf(codes.InvalidArgument, "priority %d must be between 1 and 5", priority)
			}
		}
		if r := filter.GetRange(); r != nil &&
			r.GetFrom() != "" && r.GetTo() != "" && r.GetFrom() >= r.GetTo() {
			return status.Error(codes.InvalidArgument, "range from must be less than range to")
		}

		if len(filter.GetTasks()) == 0 {
			tasks = append(tasks, dummyTaskNames...)
		} else {
			seen := make(map[string]bool)
			for _, task := range filter.GetTasks() {
				if task == "" || task == "missing" || seen[task] {
					continue
					}
				if dummyEntry(task) != nil {
					tasks = append(tasks, task)
					seen[task] = true
				}
			}
		}

		filtered := make([]string, 0, len(tasks))
		for _, task := range tasks {
			entry := dummyEntry(task)
			if entry == nil {
				continue
			}
			body := entry.GetEntryBody()

			if len(filter.GetParticipants()) > 0 && !containsParticipant(body.GetParticipants(), filter.GetParticipants()) {
				continue
			}
			if len(filter.GetPriorities()) > 0 && !containsInt32(filter.GetPriorities(), body.GetPriority()) {
				continue
			}
			if filter.GetByCompleted() && !body.GetCompleted() {
				continue
			}
			if filter.GetByOpen() && body.GetCompleted() {
				continue
			}
			if filter.GetPrefix() != "" && !strings.HasPrefix(task, filter.GetPrefix()) {
				continue
			}
			if r := filter.GetRange(); r != nil &&
				((r.GetFrom() != "" && task < r.GetFrom()) ||
					(r.GetTo() != "" && task >= r.GetTo())) {
				continue
			}
			filtered = append(filtered, task)
				}
		tasks = filtered

		sortTasks(tasks, filter.GetSort())
		if n := int(filter.GetLimit()); n > 0 && n < len(tasks) {
			tasks = tasks[:n]
		}

		for _, entry := range dummyEntryList(tasks, filter.GetTasksOnly(), s.NodeID) {
			if err := stream.Send(&stdlv1.ReadResponse{Entry: entry}); err != nil {
				return err
							}
						}
		return nil

	case nil:
		return status.Error(codes.InvalidArgument, "read selector is required")
	default:
		return status.Error(codes.InvalidArgument, "unsupported read selector")
	}
}

func containsParticipant(have, wanted []string) bool {
	for _, participant := range wanted {
		for _, candidate := range have {
			if candidate == participant {
				return true
			}
				}
			}
	return false
}

func containsInt32(have []int32, wanted int32) bool {
	for _, candidate := range have {
		if candidate == wanted {
			return true
		}
	}
	return false
}

func sortTasks(tasks []string, order *stdlv1.SortOrder) {
	sort.Slice(tasks, func(i, j int) bool {
		left, right := dummyEntry(tasks[i]), dummyEntry(tasks[j])
		if order == nil {
			return tasks[i] < tasks[j]
		}

		switch order.GetField() {
		case stdlv1.SortOrder_FIELD_TASK:
			if tasks[i] != tasks[j] {
				if order.GetDescending() {
					return tasks[i] > tasks[j]
	}
				return tasks[i] < tasks[j]
			}
		case stdlv1.SortOrder_FIELD_VERSION:
			if left.GetEntryBody().GetVersion() != right.GetEntryBody().GetVersion() {
				if order.GetDescending() {
					return left.GetEntryBody().GetVersion() > right.GetEntryBody().GetVersion()
				}
				return left.GetEntryBody().GetVersion() < right.GetEntryBody().GetVersion()
			}
		case stdlv1.SortOrder_FIELD_PRIORITY:
			if left.GetEntryBody().GetPriority() != right.GetEntryBody().GetPriority() {
				if order.GetDescending() {
					return left.GetEntryBody().GetPriority() > right.GetEntryBody().GetPriority()
				}
				return left.GetEntryBody().GetPriority() < right.GetEntryBody().GetPriority()
		}
	}
		return tasks[i] < tasks[j]
	})
}

// Update is a client-streaming RPC; all dummy updates are validated before a reply.
func (s *Server) Update(stream stdlv1.STDList_UpdateServer) error {
	reqs, err := recvAll[stdlv1.UpdateRequest](stream.Recv)
	if err != nil {
		return err
	}
	if len(reqs) == 0 {
		return status.Error(codes.InvalidArgument, "at least one update is required")
	}

	// Validate the complete batch before computing or returning any entries.
	for _, req := range reqs {
		if req == nil {
			return status.Error(codes.InvalidArgument, "update request cannot be null")
		}
		if req.GetTask() == "" {
			return status.Error(codes.InvalidArgument, "task is required")
		}
		entry := dummyEntry(req.GetTask())
		if entry == nil {
			if req.IfVersion != nil && req.GetIfVersion() != 0 {
				return status.Errorf(codes.FailedPrecondition, "task %q does not exist", req.GetTask())
			}
		} else {
			if req.IfVersion != nil {
				if req.GetIfVersion() == 0 {
					return status.Errorf(codes.FailedPrecondition, "task %q already exists", req.GetTask())
				}
				if req.GetIfVersion() != dummyVersion {
					return status.Errorf(codes.FailedPrecondition, "task %q is at version %d, not %d",
						req.GetTask(), dummyVersion, req.GetIfVersion())
		}
	}
		}
		if req.GetOp() == nil {
			return status.Errorf(codes.InvalidArgument, "task %q: no update operation", req.GetTask())
		}
		switch op := req.GetOp().(type) {
		case *stdlv1.UpdateRequest_EntryBody:
			if err := validateDummyBody(op.EntryBody); err != nil {
				return status.Errorf(codes.InvalidArgument, "task %q: %v", req.GetTask(), err)
			}
		case *stdlv1.UpdateRequest_EntryMutable:
			if action, ok := op.EntryMutable.GetAction().(*stdlv1.EntryMutables_ModifyFields); ok &&
				action.ModifyFields != nil {
				if err := validateDummyBody(action.ModifyFields.GetEntryBody()); err != nil {
					return status.Errorf(codes.InvalidArgument, "task %q: %v", req.GetTask(), err)
				}
			}
		}
	}

	out := &stdlv1.UpdateResponse{}
	for _, req := range reqs {
		entry := dummyEntry(req.GetTask())
		created := entry == nil
		body := baseDummyBody(entry)

		switch op := req.GetOp().(type) {
		case *stdlv1.UpdateRequest_EntryBody:
			if op.EntryBody == nil {
				return status.Errorf(codes.InvalidArgument, "task %q: entryBody is required", req.GetTask())
			}
			body = cloneEntryBody(op.EntryBody)
			body.Version = baseDummyBody(entry).GetVersion() + 1

		case *stdlv1.UpdateRequest_EntryMutable:
			body, err = applyDummyMutation(body, op.EntryMutable)
			if err != nil {
				return status.Errorf(codes.InvalidArgument, "task %q: %v", req.GetTask(), err)
			}
			body.Version++
		default:
			return status.Errorf(codes.InvalidArgument, "task %q: unsupported update operation", req.GetTask())
		}
		body.Description = dummyDescription(body.GetDescription(), s.NodeID)
		out.Entries = append(out.Entries, &stdlv1.Entry{Task: req.GetTask(), EntryBody: body})
		out.Created = append(out.Created, created)
	}
	return stream.SendAndClose(out)
}

func baseDummyBody(entry *stdlv1.Entry) *stdlv1.EntryBody {
	if entry != nil && entry.GetEntryBody() != nil {
		return cloneEntryBody(entry.GetEntryBody())
	}
	return &stdlv1.EntryBody{Version: dummyVersion}
}

func cloneEntryBody(body *stdlv1.EntryBody) *stdlv1.EntryBody {
	if body == nil {
		return &stdlv1.EntryBody{Version: dummyVersion}
	}
	return &stdlv1.EntryBody{
		Name: body.GetName(), Description: body.GetDescription(),
		Participants: append([]string(nil), body.GetParticipants()...),
		Priority:     body.GetPriority(), Completed: body.GetCompleted(),
		Version: body.GetVersion(),
	}
}

func validateDummyBody(body *stdlv1.EntryBody) error {
	if body == nil {
		return nil
	}
	if body.GetPriority() != 0 && (body.GetPriority() < 1 || body.GetPriority() > 5) {
		return fmt.Errorf("priority must be between 1 and 5")
	}
	if len(body.GetParticipants()) > 5 {
		return fmt.Errorf("participants cannot contain more than 5 entries")
	}
	return nil
}

func applyDummyMutation(body *stdlv1.EntryBody, mutables *stdlv1.EntryMutables) (*stdlv1.EntryBody, error) {
	if mutables == nil {
		return nil, errors.New("entryMutables is required")
	}
	switch action := mutables.GetAction().(type) {
	case *stdlv1.EntryMutables_ModifyFields:
		if action.ModifyFields == nil {
			return nil, errors.New("modifyFields is required")
		}
		modify := action.ModifyFields
		if modify.GetNameMutable() {
			body.Name = modify.GetEntryBody().GetName()
		}
		if modify.GetDescriptionMutable() {
			body.Description = modify.GetEntryBody().GetDescription()
		}
		if modify.GetParticipantsMutable() {
			body.Participants = append([]string(nil), modify.GetEntryBody().GetParticipants()...)
		}
		if modify.GetPriorityMutable() {
			body.Priority = modify.GetEntryBody().GetPriority()
		}
		if modify.GetCompletedMutable() {
			body.Completed = modify.GetEntryBody().GetCompleted()
		}
		if modify.GetVersionMutable() {
			body.Version = modify.GetEntryBody().GetVersion()
		}
		if modify.GetAppendParticipants() {
			body.Participants = append(body.Participants, modify.GetEntryBody().GetParticipants()...)
		}
	case *stdlv1.EntryMutables_AppendParticipants:
		if action.AppendParticipants == nil {
			return nil, errors.New("appendParticipants is required")
		}
		body.Participants = append(body.Participants, action.AppendParticipants.GetParticipants()...)
	case *stdlv1.EntryMutables_IncrementVersion:
		// The write itself increments the version below.
	default:
		return nil, errors.New("exactly one mutation action is required")
	}
	if err := validateDummyBody(body); err != nil {
		return nil, err
	}
	return body, nil
}

// Delete is a client-streaming RPC. Missing dummy tasks are successful no-ops.
func (s *Server) Delete(stream stdlv1.STDList_DeleteServer) error {
	reqs, err := recvAll[stdlv1.DeleteRequest](stream.Recv)
	if err != nil {
		return err
	}
	if len(reqs) == 0 {
		return status.Error(codes.InvalidArgument, "at least one delete is required")
	}

	seen := make(map[string]bool)
	var deleted int32
	for _, req := range reqs {
		if req == nil {
			return status.Error(codes.InvalidArgument, "delete request cannot be null")
		}
		if req.GetTask() == "" {
			return status.Error(codes.InvalidArgument, "task is required")
		}

		entry := dummyEntry(req.GetTask())
		if entry == nil {
			if req.IfVersion != nil && req.GetIfVersion() != 0 {
				return status.Errorf(codes.FailedPrecondition, "task %q does not exist", req.GetTask())
			}
			continue
		}
		if req.IfVersion != nil {
			if req.GetIfVersion() == 0 {
				return status.Errorf(codes.FailedPrecondition, "task %q already exists", req.GetTask())
			}
			if req.GetIfVersion() != dummyVersion {
				return status.Errorf(codes.FailedPrecondition, "task %q is at version %d, not %d",
					req.GetTask(), dummyVersion, req.GetIfVersion())
			}
		}
		if !seen[req.GetTask()] {
			seen[req.GetTask()] = true
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
