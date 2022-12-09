package controllers

import (
	v1 "github.com/kubeprj/database-operator/api/v1"
	"k8s.io/client-go/tools/record"
)

type Recorder interface {
	NormalEvent(*v1.DatabaseAccount, RecorderReason, string)
	WarningEvent(*v1.DatabaseAccount, RecorderReason, string)
	// Event(*v1.DatabaseAccount, RecorderEventType, RecorderReason, string)
}

type RecorderWrapper struct {
	r record.EventRecorder
}

// Event constructs an event from the given information and puts it in the queue for sending.
// 'object' is the object this event is about. Event will make a reference-- or you may also
// pass a reference to the object directly.
// 'eventtype' of this event, and can be one of Normal, Warning. New types could be added in future
// 'reason' is the reason this event is generated. 'reason' should be short and unique; it
// should be in UpperCamelCase format (starting with a capital letter). "reason" will be used
// to automate handling of events, so imagine people writing switch statements to handle them.
// You want to make that easy.
// 'message' is intended to be human readable.
//
// The resulting event will be created in the same namespace as the reference object.

func NewRecorder(r record.EventRecorder) Recorder {
	return &RecorderWrapper{
		r: r,
	}
}

func (r *RecorderWrapper) NormalEvent(dbAccount *v1.DatabaseAccount, reason RecorderReason, message string) {
	r.Event(
		dbAccount,
		RecorderNormal,
		reason,
		message,
	)
}

func (r *RecorderWrapper) WarningEvent(dbAccount *v1.DatabaseAccount, reason RecorderReason, message string) {
	r.Event(
		dbAccount,
		RecorderWarning,
		reason,
		message,
	)
}

func (r *RecorderWrapper) Event(dbAccount *v1.DatabaseAccount, eventType RecorderEventType, reason RecorderReason, message string) {
	r.r.Event(
		dbAccount,
		eventType.String(),
		reason.String(),
		message,
	)
}

type RecorderEventType string

func (r RecorderEventType) String() string {
	return string(r)
}

const (
	RecorderNormal  RecorderEventType = "Normal"
	RecorderWarning RecorderEventType = "Warning"
)

type RecorderReason string

func (r RecorderReason) String() string {
	return string(r)
}

const (
	ReasonQueued         RecorderReason = "Queued"
	ReasonUserCreate     RecorderReason = "UserCreate"
	ReasonDatabaseCreate RecorderReason = "DatabaseCreate"
	ReasonReady          RecorderReason = "Ready"
)
