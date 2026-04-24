package topology

import (
	"bytes"
	"encoding/hex"
	"math"
	"slices"
)

const (
	attrHTTPRequestMethod      = "http.request.method"
	attrHTTPRoute              = "http.route"
	attrRPCService             = "rpc.service"
	attrRPCMethod              = "rpc.method"
	attrMessagingOperationType = "messaging.operation.type"
	attrMessagingDestination   = "messaging.destination.name"
	unnamedOperation           = "unnamed_operation"
	rootIDSeparator            = "::"
	initialServiceHopDepth     = 1
)

// Span is the topology input model for one finalized span.
type Span struct {
	SpanID       [8]byte
	ParentSpanID [8]byte
	Service      string
	Name         string
	Kind         int32
	StartTimeNs  int64
	EndTimeNs    int64
	StatusError  bool
	OpAttrs      map[string]string
}

// Compute reconstructs trace topology and returns one metrics row per
// authentic root, plus the number of spans unreachable from any root.
func Compute(traceID [16]byte, spans map[[8]byte]Span) ([]RootMetrics, int32) {
	if len(spans) == 0 {
		return nil, 0
	}

	childrenByParent, roots := indexSpans(spans)
	visited := make(map[[8]byte]struct{}, len(spans))
	rows := make([]RootMetrics, 0, len(roots))
	traceIDHex := hex.EncodeToString(traceID[:])

	for _, rootID := range roots {
		if _, ok := visited[rootID]; ok {
			continue
		}
		rows = append(rows, computeRoot(traceIDHex, rootID, spans, childrenByParent, visited))
	}

	return rows, safeInt32(len(spans) - len(visited))
}

func indexSpans(spans map[[8]byte]Span) (map[[8]byte][][8]byte, [][8]byte) {
	childrenByParent := make(map[[8]byte][][8]byte)
	roots := make([][8]byte, 0, 1)

	for spanID, span := range spans {
		if span.ParentSpanID == ([8]byte{}) {
			roots = append(roots, spanID)
			continue
		}
		if _, ok := spans[span.ParentSpanID]; ok {
			childrenByParent[span.ParentSpanID] = append(childrenByParent[span.ParentSpanID], spanID)
		}
	}

	sortSpanIDs(roots)
	for parentID := range childrenByParent {
		sortSpanIDs(childrenByParent[parentID])
	}

	return childrenByParent, roots
}

type frame struct {
	spanID [8]byte
	depth  int32
}

type rootAccumulator struct {
	breadth         int32
	serviceHopDepth int32
	spanCount       int32
	errorCount      int32
	services        map[string]struct{}
	operations      map[operationKey]struct{}
}

type operationKey struct {
	service string
	op      string
}

func computeRoot(
	traceID string,
	rootID [8]byte,
	spans map[[8]byte]Span,
	childrenByParent map[[8]byte][][8]byte,
	visited map[[8]byte]struct{},
) RootMetrics {
	root := spans[rootID]
	acc := rootAccumulator{
		serviceHopDepth: initialServiceHopDepth,
		services:        make(map[string]struct{}),
		operations:      make(map[operationKey]struct{}),
	}
	stack := []frame{{spanID: rootID, depth: initialServiceHopDepth}}

	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		if _, ok := visited[current.spanID]; ok {
			continue
		}
		visited[current.spanID] = struct{}{}

		span := spans[current.spanID]
		acc.recordSpan(span, current.depth, childrenByParent[current.spanID])

		children := childrenByParent[current.spanID]
		for i := len(children) - 1; i >= 0; i-- {
			childID := children[i]
			child := spans[childID]
			depth := current.depth
			if child.Service != span.Service {
				depth++
			}
			stack = append(stack, frame{spanID: childID, depth: depth})
		}
	}

	rootOperation := deriveOperation(root)
	return RootMetrics{
		RootID:          root.Service + rootIDSeparator + rootOperation,
		TraceID:         traceID,
		RootService:     root.Service,
		RootOperation:   rootOperation,
		Breadth:         acc.breadth,
		ServiceHopDepth: acc.serviceHopDepth,
		ServiceCount:    safeInt32(len(acc.services)),
		OperationCount:  safeInt32(len(acc.operations)),
		SpanCount:       acc.spanCount,
		ErrorCount:      acc.errorCount,
		RootDurationNS:  root.EndTimeNs - root.StartTimeNs,
	}
}

func (a *rootAccumulator) recordSpan(span Span, depth int32, children [][8]byte) {
	a.spanCount++
	if span.StatusError {
		a.errorCount++
	}
	a.breadth = max(a.breadth, safeInt32(len(children)))
	a.serviceHopDepth = max(a.serviceHopDepth, depth)
	a.services[span.Service] = struct{}{}
	a.operations[operationKey{service: span.Service, op: deriveOperation(span)}] = struct{}{}
}

func deriveOperation(span Span) string {
	if operation, ok := deriveHTTPOperation(span); ok {
		return operation
	}
	if operation, ok := deriveRPCOperation(span); ok {
		return operation
	}
	if operation, ok := deriveMessagingOperation(span); ok {
		return operation
	}
	return fallbackOperation(span.Name)
}

func deriveHTTPOperation(span Span) (string, bool) {
	method := span.OpAttrs[attrHTTPRequestMethod]
	if method == "" {
		return "", false
	}
	if route := span.OpAttrs[attrHTTPRoute]; route != "" {
		return method + " " + route, true
	}
	return fallbackOperation(span.Name), true
}

func deriveRPCOperation(span Span) (string, bool) {
	rpcService := span.OpAttrs[attrRPCService]
	rpcMethod := span.OpAttrs[attrRPCMethod]
	if rpcService == "" || rpcMethod == "" {
		return "", false
	}
	return rpcService + "/" + rpcMethod, true
}

func deriveMessagingOperation(span Span) (string, bool) {
	messagingOperation := span.OpAttrs[attrMessagingOperationType]
	messagingDestination := span.OpAttrs[attrMessagingDestination]
	if messagingOperation == "" || messagingDestination == "" {
		return "", false
	}
	return messagingOperation + " " + messagingDestination, true
}

func fallbackOperation(name string) string {
	if name == "" {
		return unnamedOperation
	}
	return name
}

func safeInt32(n int) int32 {
	if n <= 0 {
		return 0
	}
	if n > math.MaxInt32 {
		return math.MaxInt32
	}
	//nolint:gosec // The value is explicitly bounded to the int32 range above.
	return int32(n)
}

func sortSpanIDs(ids [][8]byte) {
	slices.SortFunc(ids, func(a, b [8]byte) int {
		return bytes.Compare(a[:], b[:])
	})
}
