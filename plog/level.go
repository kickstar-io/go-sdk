package plog

const (
	// DebugLevel defines debug plog level.
	DebugLevel Level = iota
	// InfoLevel defines info plog level.
	InfoLevel
	// WarnLevel defines warn plog level.
	WarnLevel
	// ErrorLevel defines error plog level.
	ErrorLevel
	// FatalLevel defines fatal plog level.
	FatalLevel
	// PanicLevel defines panic plog level.
	PanicLevel
	// NoLevel defines an absent plog level.
	NoLevel
	// Disabled disables the logger.
	Disabled

	// TraceLevel defines trace plog level.
	TraceLevel Level = -1
	// Values less than TraceLevel are handled as numbers.
)
