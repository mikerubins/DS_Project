// Package httpapi translates the REST API to the STDList gRPC API.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/mikerubins/DS_Project/stdl/gen/rest"
	stdlv1 "github.com/mikerubins/DS_Project/stdl/gen/stdl/v1"
)

type Handlers struct {
	STDL stdlv1.STDListClient
}

var _ rest.StrictServerInterface = (*Handlers)(nil)

func NewHTTPHandler(stdl stdlv1.STDListClient, mux *http.ServeMux) http.Handler {
	strict := rest.NewStrictHandlerWithOptions(&Handlers{STDL: stdl}, nil, rest.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			writeProblem(w, http.StatusBadRequest, err.Error())
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			writeProblem(w, httpStatusFromGRPC(err), errorMessage(err))
		},
	})
	return rest.HandlerFromMux(strict, mux)
}

func (h *Handlers) GetTasks(ctx context.Context, req rest.GetTasksRequestObject) (rest.GetTasksResponseObject, error) {
	p := req.Params
	multiple := &stdlv1.ReadMultipleRequests{
		Tasks:        deref(p.Task),
		Participants: deref(p.Participant),
		Priorities:   deref(p.Priority),
		Prefix:       deref(p.Prefix),
		Range:        &stdlv1.TaskRange{From: deref(p.From), To: deref(p.To)},
		Sort:         toProtoSort(p.Sort),
		Limit:        deref(p.Limit),
		TasksOnly:    deref(p.TasksOnly),
		Consistency:  toProtoConsistency(p.Consistency),
	}
	if p.ByCompleted != nil {
		multiple.ByCompleted = *p.ByCompleted
	}
	if p.ByOpen != nil {
		multiple.ByOpen = *p.ByOpen
	}
	if multiple.ByCompleted && multiple.ByOpen {
		return nil, status.Error(codes.InvalidArgument, "byCompleted and byOpen cannot both be true")
	}
	if multiple.Limit < 0 {
		return nil, status.Error(codes.InvalidArgument, "limit cannot be negative")
	}
	for _, priority := range multiple.Priorities {
		if priority < 1 || priority > 5 {
			return nil, status.Errorf(codes.InvalidArgument, "priority %d must be between 1 and 5", priority)
		}
	}
	if multiple.GetRange().GetFrom() != "" && multiple.GetRange().GetTo() != "" &&
		multiple.GetRange().GetFrom() >= multiple.GetRange().GetTo() {
		return nil, status.Error(codes.InvalidArgument, "range from must be less than range to")
	}

	stream, err := h.STDL.Read(ctx, &stdlv1.ReadRequest{
		Select: &stdlv1.ReadRequest_MultipleRequests{MultipleRequests: multiple},
	})
	if err != nil {
		return nil, err
	}
	entries, err := recvEntries(stream)
	if err != nil {
		return nil, err
	}
	return rest.GetTasks200JSONResponse{Entries: entries}, nil
}

func (h *Handlers) SetTasks(ctx context.Context, req rest.SetTasksRequestObject) (rest.SetTasksResponseObject, error) {
	if req.Body == nil {
		return nil, status.Error(codes.InvalidArgument, "request body is required")
	}
	if len(req.Body.Entries) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one entry is required")
	}

	updates := make([]*stdlv1.UpdateRequest, 0, len(req.Body.Entries))
	for _, item := range req.Body.Entries {
		if item.Task == "" {
			return nil, status.Error(codes.InvalidArgument, "task is required")
		}
		body := entryBodyToProto(item.EntryBody)
		if err := validateRESTBody(item.EntryBody); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "task %q: %v", item.Task, err)
		}
		updates = append(updates, &stdlv1.UpdateRequest{
			Task:      item.Task,
			Op:        &stdlv1.UpdateRequest_EntryBody{EntryBody: body},
			IfVersion: item.IfVersion,
		})
	}
	out, err := h.update(ctx, updates)
	if err != nil {
		return nil, err
	}
	return rest.SetTasks200JSONResponse{Entries: entriesFromProto(out.GetEntries())}, nil
}

func (h *Handlers) ModifyTasks(ctx context.Context, req rest.ModifyTasksRequestObject) (rest.ModifyTasksResponseObject, error) {
	if req.Body == nil {
		return nil, status.Error(codes.InvalidArgument, "request body is required")
	}
	if len(req.Body.Ops) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one operation is required")
	}

	updates := make([]*stdlv1.UpdateRequest, 0, len(req.Body.Ops))
	for _, item := range req.Body.Ops {
		if item.Task == "" {
			return nil, status.Error(codes.InvalidArgument, "task is required")
		}
		mutables, err := entryMutablesToProto(item.EntryMutables)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "task %q: %v", item.Task, err)
		}
		updates = append(updates, &stdlv1.UpdateRequest{
			Task:      item.Task,
			Op:        &stdlv1.UpdateRequest_EntryMutable{EntryMutable: mutables},
			IfVersion: item.IfVersion,
		})
	}
	out, err := h.update(ctx, updates)
	if err != nil {
		return nil, err
	}
	return rest.ModifyTasks200JSONResponse{Entries: entriesFromProto(out.GetEntries())}, nil
}

func (h *Handlers) DeleteTasks(ctx context.Context, req rest.DeleteTasksRequestObject) (rest.DeleteTasksResponseObject, error) {
	if len(req.Params.Task) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one task is required")
	}
	deletes := make([]*stdlv1.DeleteRequest, 0, len(req.Params.Task))
	seen := make(map[string]bool)
	for _, task := range req.Params.Task {
		if task == "" {
			return nil, status.Error(codes.InvalidArgument, "task cannot be empty")
		}
		if seen[task] {
			continue
		}
		seen[task] = true
		deletes = append(deletes, &stdlv1.DeleteRequest{Task: task})
	}
	out, err := h.delete(ctx, deletes)
	if err != nil {
		return nil, err
	}
	return rest.DeleteTasks200JSONResponse{Deleted: out.GetDeleted()}, nil
}

func (h *Handlers) GetTask(ctx context.Context, req rest.GetTaskRequestObject) (rest.GetTaskResponseObject, error) {
	if req.Task == "" {
		return nil, status.Error(codes.InvalidArgument, "task is required")
	}
	stream, err := h.STDL.Read(ctx, &stdlv1.ReadRequest{
		Select: &stdlv1.ReadRequest_OneRequest{
			OneRequest: &stdlv1.ReadOneRequest{Task: req.Task},
		},
	})
	if err != nil {
		return nil, err
	}
	message, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return nil, status.Errorf(codes.NotFound, "task %q not found", req.Task)
	}
	if err != nil {
		return nil, err
	}
	entry := message.GetEntry()
	if entry == nil {
		return nil, status.Errorf(codes.Internal, "server returned no entry for task %q", req.Task)
	}
	body := entryFromProto(entry)
	var etag *string
	if entry.GetEntryBody() != nil {
		value := etagForVersion(entry.GetEntryBody().GetVersion())
		etag = &value
	}
	return rest.GetTask200JSONResponse{
		Body:    body,
		Headers: rest.GetTask200ResponseHeaders{ETag: etag},
	}, nil
}

func (h *Handlers) SetTask(ctx context.Context, req rest.SetTaskRequestObject) (rest.SetTaskResponseObject, error) {
	if req.Task == "" {
		return nil, status.Error(codes.InvalidArgument, "task is required")
	}
	if req.Body == nil {
		return nil, status.Error(codes.InvalidArgument, "request body is required")
	}
	if err := validateRESTBody(req.Body.EntryBody); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "task %q: %v", req.Task, err)
	}
	ifVersion, err := versionFromIfMatch(req.Params.IfMatch)
	if err != nil {
		return nil, err
	}
	out, err := h.update(ctx, []*stdlv1.UpdateRequest{{
		Task: req.Task,
		Op: &stdlv1.UpdateRequest_EntryBody{
			EntryBody: entryBodyToProto(req.Body.EntryBody),
		},
		IfVersion: ifVersion,
	}})
	if err != nil {
		return nil, err
	}
	if len(out.GetEntries()) != 1 {
		return nil, status.Error(codes.Internal, "server returned an invalid update response")
	}
	entry := entryFromProto(out.GetEntries()[0])
	if len(out.GetCreated()) == 1 && out.GetCreated()[0] {
		return rest.SetTask201JSONResponse(entry), nil
	}
	return rest.SetTask200JSONResponse(entry), nil
}

func (h *Handlers) ModifyTask(ctx context.Context, req rest.ModifyTaskRequestObject) (rest.ModifyTaskResponseObject, error) {
	if req.Task == "" {
		return nil, status.Error(codes.InvalidArgument, "task is required")
	}
	if req.Body == nil {
		return nil, status.Error(codes.InvalidArgument, "request body is required")
	}
	mutables, err := entryMutablesToProto(req.Body.EntryMutables)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ifVersion, err := versionFromIfMatch(req.Params.IfMatch)
	if err != nil {
		return nil, err
	}
	out, err := h.update(ctx, []*stdlv1.UpdateRequest{{
		Task:      req.Task,
		Op:        &stdlv1.UpdateRequest_EntryMutable{EntryMutable: mutables},
		IfVersion: ifVersion,
	}})
	if err != nil {
		return nil, err
	}
	if len(out.GetEntries()) != 1 {
		return nil, status.Error(codes.Internal, "server returned an invalid update response")
	}
	return rest.ModifyTask200JSONResponse(entryFromProto(out.GetEntries()[0])), nil
}

func (h *Handlers) DeleteTask(ctx context.Context, req rest.DeleteTaskRequestObject) (rest.DeleteTaskResponseObject, error) {
	if req.Task == "" {
		return nil, status.Error(codes.InvalidArgument, "task is required")
	}
	ifVersion, err := versionFromIfMatch(req.Params.IfMatch)
	if err != nil {
		return nil, err
	}
	if _, err := h.delete(ctx, []*stdlv1.DeleteRequest{{Task: req.Task, IfVersion: ifVersion}}); err != nil {
		return nil, err
	}
	return rest.DeleteTask204Response{}, nil
}

func recvEntries(stream stdlv1.STDList_ReadClient) ([]rest.Entry, error) {
	entries := make([]rest.Entry, 0)
	for {
		message, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		if message == nil || message.GetEntry() == nil {
			return nil, status.Error(codes.Internal, "server returned an empty read response")
		}
		entries = append(entries, entryFromProto(message.GetEntry()))
	}
}

func (h *Handlers) update(ctx context.Context, updates []*stdlv1.UpdateRequest) (*stdlv1.UpdateResponse, error) {
	stream, err := h.STDL.Update(ctx)
	if err != nil {
		return nil, err
	}
	for _, update := range updates {
		if err := stream.Send(update); err != nil {
			return stream.CloseAndRecv()
		}
	}
	return stream.CloseAndRecv()
}

func (h *Handlers) delete(ctx context.Context, deletes []*stdlv1.DeleteRequest) (*stdlv1.DeleteResponse, error) {
	stream, err := h.STDL.Delete(ctx)
	if err != nil {
		return nil, err
	}
	for _, deleteRequest := range deletes {
		if err := stream.Send(deleteRequest); err != nil {
			return stream.CloseAndRecv()
		}
	}
	return stream.CloseAndRecv()
}

func entryMutablesToProto(mutables rest.EntryMutables) (*stdlv1.EntryMutables, error) {
	count := 0
	if mutables.ModifyFields != nil {
		count++
	}
	if mutables.AppendParticipants != nil {
		count++
	}
	if mutables.IncrementVersion != nil {
		if !*mutables.IncrementVersion {
			return nil, errors.New("incrementVersion must be true")
		}
		count++
	}
	if count != 1 {
		return nil, errors.New("give exactly one of modifyFields, appendParticipants, or incrementVersion")
	}

	if mutables.ModifyFields != nil {
		modify := mutables.ModifyFields
		action := &stdlv1.ModifyAction{
			NameMutable:         deref(modify.NameMutable),
			DescriptionMutable:  deref(modify.DescriptionMutable),
			ParticipantsMutable: deref(modify.ParticipantsMutable),
			PriorityMutable:     deref(modify.PriorityMutable),
			CompletedMutable:    deref(modify.CompletedMutable),
			VersionMutable:      deref(modify.VersionMutable),
			AppendParticipants:  deref(modify.AppendParticipants),
		}
		if modify.EntryBody != nil {
			action.EntryBody = entryBodyToProto(*modify.EntryBody)
			if err := validateRESTBody(*modify.EntryBody); err != nil {
				return nil, err
			}
		}
		return &stdlv1.EntryMutables{
			Action: &stdlv1.EntryMutables_ModifyFields{ModifyFields: action},
		}, nil
	}
	if mutables.AppendParticipants != nil {
		participants := deref(mutables.AppendParticipants.Participants)
		if len(participants) == 0 {
			return nil, errors.New("appendParticipants.participants cannot be empty")
		}
		if len(participants) > 5 {
			return nil, errors.New("appendParticipants.participants cannot contain more than 5 entries")
		}
		return &stdlv1.EntryMutables{
			Action: &stdlv1.EntryMutables_AppendParticipants{
				AppendParticipants: &stdlv1.AppendAction{Participants: participants},
			},
		}, nil
	}
	return &stdlv1.EntryMutables{
		Action: &stdlv1.EntryMutables_IncrementVersion{IncrementVersion: true},
	}, nil
}

func toProtoConsistency[T ~string](value *T) stdlv1.Consistency {
	if value != nil && string(*value) == "linearizable" {
		return stdlv1.Consistency_CONSISTENCY_LINEARIZABLE
	}
	return stdlv1.Consistency_CONSISTENCY_SEQUENTIAL
}

func toProtoSort[T ~string](value *T) *stdlv1.SortOrder {
	if value == nil {
		return &stdlv1.SortOrder{Field: stdlv1.SortOrder_FIELD_TASK}
	}
	name, descending := strings.CutPrefix(string(*value), "-")
	order := &stdlv1.SortOrder{Descending: descending}
	switch name {
	case "version":
		order.Field = stdlv1.SortOrder_FIELD_VERSION
	case "priority":
		order.Field = stdlv1.SortOrder_FIELD_PRIORITY
	default:
		order.Field = stdlv1.SortOrder_FIELD_TASK
	}
	return order
}

func entryBodyToProto(body rest.EntryBody) *stdlv1.EntryBody {
	return &stdlv1.EntryBody{
		Name:         deref(body.Name),
		Description:  deref(body.Description),
		Participants: deref(body.Participants),
		Priority:     deref(body.Priority),
		Completed:    deref(body.Completed),
		Version:      deref(body.Version),
	}
}

func validateRESTBody(body rest.EntryBody) error {
	if body.Priority != nil && (*body.Priority < 1 || *body.Priority > 5) {
		return fmt.Errorf("priority must be between 1 and 5")
	}
	if body.Participants != nil && len(*body.Participants) > 5 {
		return errors.New("participants cannot contain more than 5 entries")
	}
	return nil
}

func entryFromProto(entry *stdlv1.Entry) rest.Entry {
	body := rest.EntryBody{}
	if entry.GetEntryBody() != nil {
		protoBody := entry.GetEntryBody()
		body = rest.EntryBody{
			Name:         stringPtr(protoBody.GetName()),
			Description:  stringPtr(protoBody.GetDescription()),
			Participants: stringSlicePtr(protoBody.GetParticipants()),
			Priority:     int32Ptr(protoBody.GetPriority()),
			Completed:    boolPtr(protoBody.GetCompleted()),
			Version:      int64Ptr(protoBody.GetVersion()),
		}
	}
	return rest.Entry{Task: entry.GetTask(), EntryBody: body}
}

func entriesFromProto(entries []*stdlv1.Entry) []rest.Entry {
	result := make([]rest.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry != nil {
			result = append(result, entryFromProto(entry))
		}
	}
	return result
}

func etagForVersion(version int64) string {
	return strconv.Quote(strconv.FormatInt(version, 10))
}

func versionFromIfMatch(header *string) (*int64, error) {
	if header == nil || strings.TrimSpace(*header) == "" {
		return nil, nil
	}
	raw := strings.TrimSpace(*header)
	raw = strings.TrimPrefix(raw, "W/")
	raw = strings.Trim(raw, `"`)
	if raw == "" {
		return nil, status.Error(codes.InvalidArgument, "If-Match cannot be empty")
	}
	version, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || version < 0 {
		return nil, status.Errorf(codes.InvalidArgument, "If-Match %q is not a valid version", *header)
	}
	return &version, nil
}

func ptr[T any](value T) *T          { return &value }
func stringPtr(value string) *string { return ptr(value) }
func int32Ptr(value int32) *int32    { return ptr(value) }
func int64Ptr(value int64) *int64    { return ptr(value) }
func boolPtr(value bool) *bool       { return ptr(value) }

func deref[T any](value *T) T {
	if value == nil {
		var zero T
		return zero
	}
	return *value
}

func stringSlicePtr(value []string) *[]string {
	if value == nil {
		return nil
	}
	copyValue := append([]string(nil), value...)
	return &copyValue
}

func httpStatusFromGRPC(err error) int {
	switch status.Code(err) {
	case codes.NotFound:
		return http.StatusNotFound
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.FailedPrecondition:
		return http.StatusPreconditionFailed
	case codes.Unimplemented:
		return http.StatusNotImplemented
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	default:
		if errors.Is(err, context.DeadlineExceeded) {
			return http.StatusGatewayTimeout
		}
		return http.StatusInternalServerError
	}
}

func errorMessage(err error) string {
	if grpcStatus, ok := status.FromError(err); ok {
		return grpcStatus.Message()
	}
	return err.Error()
}

func writeProblem(w http.ResponseWriter, code int, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(rest.Problem{
		Title:  http.StatusText(code),
		Status: code,
		Detail: &detail,
	})
}
