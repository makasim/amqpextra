// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/makasim/amqpextra/publisher (interfaces: ConnectionReady,AMQPConnection,AMQPChannel)

// Package mock_publisher is a generated GoMock package.
package mock_publisher

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	publisher "github.com/makasim/amqpextra/publisher"
	amqp "github.com/streadway/amqp"
)

// MockConnectionReady is a mock of ConnectionReady interface.
type MockConnectionReady struct {
	ctrl     *gomock.Controller
	recorder *MockConnectionReadyMockRecorder
}

// MockConnectionReadyMockRecorder is the mock recorder for MockConnectionReady.
type MockConnectionReadyMockRecorder struct {
	mock *MockConnectionReady
}

// NewMockConnectionReady creates a new mock instance.
func NewMockConnectionReady(ctrl *gomock.Controller) *MockConnectionReady {
	mock := &MockConnectionReady{ctrl: ctrl}
	mock.recorder = &MockConnectionReadyMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConnectionReady) EXPECT() *MockConnectionReadyMockRecorder {
	return m.recorder
}

// Conn mocks base method.
func (m *MockConnectionReady) Conn() publisher.AMQPConnection {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Conn")
	ret0, _ := ret[0].(publisher.AMQPConnection)
	return ret0
}

// Conn indicates an expected call of Conn.
func (mr *MockConnectionReadyMockRecorder) Conn() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Conn", reflect.TypeOf((*MockConnectionReady)(nil).Conn))
}

// NotifyClose mocks base method.
func (m *MockConnectionReady) NotifyClose() chan struct{} {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyClose")
	ret0, _ := ret[0].(chan struct{})
	return ret0
}

// NotifyClose indicates an expected call of NotifyClose.
func (mr *MockConnectionReadyMockRecorder) NotifyClose() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyClose", reflect.TypeOf((*MockConnectionReady)(nil).NotifyClose))
}

// NotifyLost mocks base method.
func (m *MockConnectionReady) NotifyLost() chan struct{} {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyLost")
	ret0, _ := ret[0].(chan struct{})
	return ret0
}

// NotifyLost indicates an expected call of NotifyLost.
func (mr *MockConnectionReadyMockRecorder) NotifyLost() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyLost", reflect.TypeOf((*MockConnectionReady)(nil).NotifyLost))
}

// MockAMQPConnection is a mock of AMQPConnection interface.
type MockAMQPConnection struct {
	ctrl     *gomock.Controller
	recorder *MockAMQPConnectionMockRecorder
}

// MockAMQPConnectionMockRecorder is the mock recorder for MockAMQPConnection.
type MockAMQPConnectionMockRecorder struct {
	mock *MockAMQPConnection
}

// NewMockAMQPConnection creates a new mock instance.
func NewMockAMQPConnection(ctrl *gomock.Controller) *MockAMQPConnection {
	mock := &MockAMQPConnection{ctrl: ctrl}
	mock.recorder = &MockAMQPConnectionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAMQPConnection) EXPECT() *MockAMQPConnectionMockRecorder {
	return m.recorder
}

// MockAMQPChannel is a mock of AMQPChannel interface.
type MockAMQPChannel struct {
	ctrl     *gomock.Controller
	recorder *MockAMQPChannelMockRecorder
}

// MockAMQPChannelMockRecorder is the mock recorder for MockAMQPChannel.
type MockAMQPChannelMockRecorder struct {
	mock *MockAMQPChannel
}

// NewMockAMQPChannel creates a new mock instance.
func NewMockAMQPChannel(ctrl *gomock.Controller) *MockAMQPChannel {
	mock := &MockAMQPChannel{ctrl: ctrl}
	mock.recorder = &MockAMQPChannelMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAMQPChannel) EXPECT() *MockAMQPChannelMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockAMQPChannel) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockAMQPChannelMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockAMQPChannel)(nil).Close))
}

// NotifyClose mocks base method.
func (m *MockAMQPChannel) NotifyClose(arg0 chan *amqp.Error) chan *amqp.Error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyClose", arg0)
	ret0, _ := ret[0].(chan *amqp.Error)
	return ret0
}

// NotifyClose indicates an expected call of NotifyClose.
func (mr *MockAMQPChannelMockRecorder) NotifyClose(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyClose", reflect.TypeOf((*MockAMQPChannel)(nil).NotifyClose), arg0)
}

// NotifyFlow mocks base method.
func (m *MockAMQPChannel) NotifyFlow(arg0 chan bool) chan bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyFlow", arg0)
	ret0, _ := ret[0].(chan bool)
	return ret0
}

// NotifyFlow indicates an expected call of NotifyFlow.
func (mr *MockAMQPChannelMockRecorder) NotifyFlow(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyFlow", reflect.TypeOf((*MockAMQPChannel)(nil).NotifyFlow), arg0)
}

// Publish mocks base method.
func (m *MockAMQPChannel) Publish(arg0, arg1 string, arg2, arg3 bool, arg4 amqp.Publishing) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Publish", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(error)
	return ret0
}

// Publish indicates an expected call of Publish.
func (mr *MockAMQPChannelMockRecorder) Publish(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Publish", reflect.TypeOf((*MockAMQPChannel)(nil).Publish), arg0, arg1, arg2, arg3, arg4)
}
