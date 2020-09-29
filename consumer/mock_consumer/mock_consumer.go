// Code generated by MockGen. DO NOT EDIT.
// Source: consumer/interfaces.go

// Package mock_consumer is a generated GoMock package.
package mock_consumer

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	consumer "github.com/makasim/amqpextra/consumer"
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
func (m *MockConnectionReady) Conn() consumer.Connection {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Conn")
	ret0, _ := ret[0].(consumer.Connection)
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

// Consume mocks base method.
func (m *MockChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Consume", queue, consumer, autoAck, exclusive, noLocal, noWait, args)
	ret0, _ := ret[0].(<-chan amqp.Delivery)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Consume indicates an expected call of Consume.
func (mr *MockChannelMockRecorder) Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Consume", reflect.TypeOf((*MockChannel)(nil).Consume), queue, consumer, autoAck, exclusive, noLocal, noWait, args)
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

// NotifyCancel mocks base method.
func (m *MockChannel) NotifyCancel(c chan string) chan string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NotifyCancel", c)
	ret0, _ := ret[0].(chan string)
	return ret0
}

// NotifyCancel indicates an expected call of NotifyCancel.
func (mr *MockChannelMockRecorder) NotifyCancel(c interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NotifyCancel", reflect.TypeOf((*MockChannel)(nil).NotifyCancel), c)
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
