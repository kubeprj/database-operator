package helper

import (
	"bytes"
	"encoding/json"
)

const (
	MethodUpdate string = "update"
	MethodCreate string = "create"
	MethodDelete string = "delete"
)

type Request struct {
	ID       string `json:"id"`
	Method   string `json:"method,omitempty"`
	Username string `json:"username"`
}

func (r *Request) Marshal() ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	if err := json.NewEncoder(buf).Encode(r); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type Response struct {
	ID       string `json:"id"`
	Error    string `json:"error,omitempty"`
	Username string `json:"username"`
	Database string `json:"database"`
	Password string `json:"password"`
}

func (r *Response) Marshal() ([]byte, error) {
	buf := bytes.NewBuffer(nil)

	if err := json.NewEncoder(buf).Encode(r); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func UnmarshalResponse(body []byte) (*Response, error) {
	var o *Response
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&o); err != nil {
		return nil, err
	}

	return o, nil
}

func UnmarshalRequest(body []byte) (*Request, error) {
	var o *Request
	if err := json.NewDecoder(bytes.NewReader(body)).Decode(&o); err != nil {
		return nil, err
	}

	return o, nil
}
