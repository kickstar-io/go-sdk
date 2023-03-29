package plog

import "github.com/rs/zerolog"

type event struct {
	*zerolog.Event
}

type Event interface {
	Msg(msg string)
	Msgf(format string, v ...interface{})
	Send()
	Str(key, val string) Event
	Int(key string, i int) Event
	Int64(key string, i int64) Event
	Float64(key string, f float64) Event
	Op(val string) Event
	Bool(key string, b bool) Event
	Var(key string, i interface{}) Event
	Err(err error) Event
}

func (e *event) Str(key, val string) Event {
	e.Event.Str(key, val)
	return e
}

func (e *event) Int(key string, i int) Event {
	e.Event.Int(key, i)
	return e
}

func (e *event) Int64(key string, i int64) Event {
	e.Event.Int64(key, i)
	return e
}

func (e *event) Float64(key string, f float64) Event {
	e.Event.Float64(key, f)
	return e
}

func (e *event) Bool(key string, b bool) Event {
	e.Event.Bool(key, b)
	return e
}

func (e *event) Op(val string) Event {
	return e.Str("operation", val)
}

func (e *event) Var(key string, i interface{}) Event {
	e.Event.Interface(key, i)
	return e
}

func (e *event) Err(err error) Event {
	e.Event.Err(err)
	return e
}
