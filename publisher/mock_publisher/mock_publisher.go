// Code generated by MockGen. DO NOT EDIT.
// Source: publisher/publisher.go

// Package mock_publisher is a generated GoMock package.
package mock_publisher

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	amqp "github.com/streadway/amqp"
)

// MockConnection is a mock of Connection interface.
type MockConnection struct {
	ctrl     *gomock.Controller
	recorder *MockConnectionMockRecorder
}

// MockConnectionMockRecorder is the mock recorder for MockConnection.
type MockConnectionMockRecorder struct {
	mock *MockConnection
}

// NewMockConnection creates a new mock instance.
func NewMockConnection(ctrl *gomock.Controller) *MockConnection {
	mock := &MockConnection{ctrl: ctrl}
	mock.recorder = &MockConnectionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConnection) EXPECT() *MockConnectionMockRecorder {
	return m.recorder
}

// NotifyClose mocks base method.
func (m *MockConnection) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyClose", receiver)
	ret0, _ := ret[0].(chan *amqp.Error)
	return ret0
}

// NotifyClose indicates an expected call of NotifyClose.
func (mr *MockConnectionMockRecorder) NotifyClose(receiver interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyClose", reflect.TypeOf((*MockConnection)(nil).NotifyClose), receiver)
}

// Close mocks base method.
func (m *MockConnection) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockConnectionMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockConnection)(nil).Close))
}

// MockChannel is a mock of Channel interface.
type MockChannel struct {
	ctrl     *gomock.Controller
	recorder *MockChannelMockRecorder
}

// MockChannelMockRecorder is the mock recorder for MockChannel.
type MockChannelMockRecorder struct {
	mock *MockChannel
}

// NewMockChannel creates a new mock instance.
func NewMockChannel(ctrl *gomock.Controller) *MockChannel {
	mock := &MockChannel{ctrl: ctrl}
	mock.recorder = &MockChannelMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockChannel) EXPECT() *MockChannelMockRecorder {
	return m.recorder
}

// Publish mocks base method.
func (m *MockChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Publish", exchange, key, mandatory, immediate, msg)
	ret0, _ := ret[0].(error)
	return ret0
}

// Publish indicates an expected call of Publish.
func (mr *MockChannelMockRecorder) Publish(exchange, key, mandatory, immediate, msg interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Publish", reflect.TypeOf((*MockChannel)(nil).Publish), exchange, key, mandatory, immediate, msg)
}

// NotifyClose mocks base method.
func (m *MockChannel) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyClose", receiver)
	ret0, _ := ret[0].(chan *amqp.Error)
	return ret0
}

// NotifyClose indicates an expected call of NotifyClose.
func (mr *MockChannelMockRecorder) NotifyClose(receiver interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyClose", reflect.TypeOf((*MockChannel)(nil).NotifyClose), receiver)
}

// Close mocks base method.
func (m *MockChannel) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockChannelMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockChannel)(nil).Close))
}