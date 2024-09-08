// Code generated by mockery v2.32.4. DO NOT EDIT.

package datacoord

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// NMockAllocator is an autogenerated mock type for the allocator type
type NMockAllocator struct {
	mock.Mock
}

type NMockAllocator_Expecter struct {
	mock *mock.Mock
}

func (_m *NMockAllocator) EXPECT() *NMockAllocator_Expecter {
	return &NMockAllocator_Expecter{mock: &_m.Mock}
}

// allocID provides a mock function with given fields: _a0
func (_m *NMockAllocator) allocID(_a0 context.Context) (int64, error) {
	ret := _m.Called(_a0)

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) (int64, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(context.Context) int64); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NMockAllocator_allocID_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'allocID'
type NMockAllocator_allocID_Call struct {
	*mock.Call
}

// allocID is a helper method to define mock.On call
//   - _a0 context.Context
func (_e *NMockAllocator_Expecter) allocID(_a0 interface{}) *NMockAllocator_allocID_Call {
	return &NMockAllocator_allocID_Call{Call: _e.mock.On("allocID", _a0)}
}

func (_c *NMockAllocator_allocID_Call) Run(run func(_a0 context.Context)) *NMockAllocator_allocID_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *NMockAllocator_allocID_Call) Return(_a0 int64, _a1 error) *NMockAllocator_allocID_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *NMockAllocator_allocID_Call) RunAndReturn(run func(context.Context) (int64, error)) *NMockAllocator_allocID_Call {
	_c.Call.Return(run)
	return _c
}

// allocN provides a mock function with given fields: n
func (_m *NMockAllocator) allocN(n int64) (int64, int64, error) {
	ret := _m.Called(n)

	var r0 int64
	var r1 int64
	var r2 error
	if rf, ok := ret.Get(0).(func(int64) (int64, int64, error)); ok {
		return rf(n)
	}
	if rf, ok := ret.Get(0).(func(int64) int64); ok {
		r0 = rf(n)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(int64) int64); ok {
		r1 = rf(n)
	} else {
		r1 = ret.Get(1).(int64)
	}

	if rf, ok := ret.Get(2).(func(int64) error); ok {
		r2 = rf(n)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// NMockAllocator_allocN_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'allocN'
type NMockAllocator_allocN_Call struct {
	*mock.Call
}

// allocN is a helper method to define mock.On call
//   - n int64
func (_e *NMockAllocator_Expecter) allocN(n interface{}) *NMockAllocator_allocN_Call {
	return &NMockAllocator_allocN_Call{Call: _e.mock.On("allocN", n)}
}

func (_c *NMockAllocator_allocN_Call) Run(run func(n int64)) *NMockAllocator_allocN_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(int64))
	})
	return _c
}

func (_c *NMockAllocator_allocN_Call) Return(_a0 int64, _a1 int64, _a2 error) *NMockAllocator_allocN_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *NMockAllocator_allocN_Call) RunAndReturn(run func(int64) (int64, int64, error)) *NMockAllocator_allocN_Call {
	_c.Call.Return(run)
	return _c
}

// allocTimestamp provides a mock function with given fields: _a0
func (_m *NMockAllocator) allocTimestamp(_a0 context.Context) (uint64, error) {
	ret := _m.Called(_a0)

	var r0 uint64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) (uint64, error)); ok {
		return rf(_a0)
	}
	if rf, ok := ret.Get(0).(func(context.Context) uint64); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Get(0).(uint64)
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(_a0)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// NMockAllocator_allocTimestamp_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'allocTimestamp'
type NMockAllocator_allocTimestamp_Call struct {
	*mock.Call
}

// allocTimestamp is a helper method to define mock.On call
//   - _a0 context.Context
func (_e *NMockAllocator_Expecter) allocTimestamp(_a0 interface{}) *NMockAllocator_allocTimestamp_Call {
	return &NMockAllocator_allocTimestamp_Call{Call: _e.mock.On("allocTimestamp", _a0)}
}

func (_c *NMockAllocator_allocTimestamp_Call) Run(run func(_a0 context.Context)) *NMockAllocator_allocTimestamp_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *NMockAllocator_allocTimestamp_Call) Return(_a0 uint64, _a1 error) *NMockAllocator_allocTimestamp_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *NMockAllocator_allocTimestamp_Call) RunAndReturn(run func(context.Context) (uint64, error)) *NMockAllocator_allocTimestamp_Call {
	_c.Call.Return(run)
	return _c
}

// NewNMockAllocator creates a new instance of NMockAllocator. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewNMockAllocator(t interface {
	mock.TestingT
	Cleanup(func())
}) *NMockAllocator {
	mock := &NMockAllocator{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}