# Simple To-Do List

Part I of a distributed-systems project: a To-Do REST API backed by an internal
gRPC service. The current gRPC service returns fixed dummy tasks; this project
does not yet persist task changes or implement replicated state.

## What is implemented

- Eight REST operations for reading, creating/replacing, modifying, and
  deleting one or multiple tasks.
- Collection filters for task IDs, participants, priorities, completion state,
  task-ID prefix/range, sorting, result limit, and task-only responses.
- Per-task versions, `ETag` on single-task reads, and optional `If-Match` /
  `ifVersion` checks on writes.
- RFC 9457 `application/problem+json` error responses.
- Three internal gRPC methods: server-streaming `Read`, client-streaming
  `Update`, and client-streaming `Delete`.
- Two-node demo: n2 serves REST on port 8081 and forwards its gRPC calls to n1
  on port 50051. The demo labels the HTTP node and shows the gRPC node in dummy
  task descriptions.

The dummy service starts each request with the same three tasks: `alpha`,
`beta`, and `gamma`. Writes and deletes return simulated results; they do not
change the task set for later requests.

### Consistency scope

The REST API exposes `sequential` and `linearizable` read options. At this
stage, both are translated to `CONSISTENCY_UNSPECIFIED` (defined as sequential
in the protobuf contract). No distributed consistency guarantee is enforced
in Part I.

## REST API

| Method and path | Behavior | Documented success statuses |
|---|---|---|
| `GET /v1/tasks` | Read a filtered, sorted task list | `200` |
| `POST /v1/tasks` | Set a batch of task bodies | `200` |
| `PATCH /v1/tasks` | Apply a batch of task mutations | `200` |
| `DELETE /v1/tasks?task=...` | Delete a batch; missing tasks are ignored | `200` |
| `GET /v1/tasks/{task}` | Read one task; returns its version in `ETag` | `200` |
| `POST /v1/tasks/{task}` | Create or replace one task | `201` created, `200` replaced |
| `PATCH /v1/tasks/{task}` | Modify one task | `200` |
| `DELETE /v1/tasks/{task}` | Delete one task; a missing task is a no-op | `204` |

Error responses documented by the OpenAPI contract include `400` for invalid
requests, `404` for a missing single task, and `412` for a failed version
precondition. See `api/openapi.yaml` for request schemas and operation details.

### Example requests

```sh
# Read tasks, optionally filtering and sorting them
curl 'http://localhost:8080/v1/tasks?participant=alice&priority=1&sort=-priority'

# Read one task and its ETag
curl -i 'http://localhost:8080/v1/tasks/alpha?consistency=sequential'

# Create or replace one task
curl -i -X POST 'http://localhost:8080/v1/tasks/demo' \
  -H 'Content-Type: application/json' \
  -d '{"entryBody":{"name":"Demo","description":"Example task","participants":["alice"],"priority":2,"completed":false}}'

# Modify one task; exactly one mutation action is required
curl -i -X PATCH 'http://localhost:8080/v1/tasks/alpha' \
  -H 'Content-Type: application/json' \
  -d '{"entryMutables":{"incrementVersion":true}}'

# Delete one task, optionally checking its version
curl -i -X DELETE 'http://localhost:8080/v1/tasks/alpha' \
  -H 'If-Match: "1"'
```

## Build and run

Requirements: Go 1.26 or newer, `make`, `curl`, and `protoc`
([installation instructions](https://protobuf.dev/installation/)). The shell
scripts and Makefile use POSIX commands; on Windows, run them from Git Bash or
WSL.

```sh
# One-time generator setup
make setup

# Build, start both nodes, and test the documented status codes on both ports
./etc/demo.sh
```

To run the nodes manually, use two terminals. Start n1 first:

```sh
make run
```

Then start n2 in the second terminal:

```sh
make run2
```

- n1 serves REST on `http://localhost:8080` and gRPC on `localhost:50051`.
- n2 serves REST on `http://localhost:8081` and forwards its REST requests to
  n1's gRPC service.
- The demo starts and stops its own processes. Do not start `make run` or
  `make run2` separately when running `./etc/demo.sh`.

## API documentation and gRPC inspection

With a node running, open:

- Swagger UI: `http://localhost:8080/docs`
- Swagger Editor: `http://localhost:8080/editor`
- OpenAPI document: `http://localhost:8080/openapi.yaml`

The editor can preview changes, but save the YAML explicitly and regenerate
the REST code afterward. Optional Docker-hosted API tools are available with
`make tools-up` and can be stopped with `make tools-down`.

The gRPC server has reflection enabled. If `grpcurl` is installed:

```sh
grpcurl -plaintext localhost:50051 list
grpcurl -plaintext -d '{"oneRequest":{"task":"alpha"}}' \
  localhost:50051 stdl.v1.STDList/Read
grpcurl -plaintext -d '{"multipleRequests":{"prefix":"a"}}' \
  localhost:50051 stdl.v1.STDList/Read
```

## Project layout

| Path | Purpose |
|---|---|
| `api/openapi.yaml` | Client-facing REST API contract |
| `api/oapi-codegen.yaml` | REST code generator configuration |
| `proto/stdl/v1/stdl.proto` | Internal gRPC service and message definitions |
| `internal/httpapi/` | REST-to-gRPC handlers and HTTP error mapping |
| `internal/grpcsvc/` | Part I dummy gRPC implementation |
| `cmd/server/` | Starts one HTTP and one gRPC server per node |
| `etc/demo.sh` | Two-node end-to-end demo and status checks |
| `gen/` | Generated REST and protobuf code; do not edit by hand |

Regenerate generated code with `make gen`; `make build` generates code, runs
`go vet`, and builds the server.
