package firebase

import "errors"

var (
	errMap = map[string]error{
		"MissingRegistration":       ErrorMissingRegistration,
		"InvalidRegistration":       ErrorInvalidRegistration,
		"NotRegistered":             ErrorNotRegistered,
		"InvalidPackageName":        ErrorInvalidPackageName,
		"MismatchSenderId":          ErrorMismatchSenderID,
		"MessageTooBig":             ErrorMessageTooBig,
		"InvalidDataKey":            ErrorInvalidDataKey,
		"InvalidTtl":                ErrorInvalidTTL,
		"Unavailable":               ErrorUnavailable,
		"InternalServerError":       ErrorInternalServerError,
		"DeviceMessageRateExceeded": ErrorDeviceMessageRateExceeded,
		"TopicsMessageRateExceeded": ErrorTopicsMessageRateExceeded,
		"InvalidParameters":         ErrorInvalidParameters,
		"InvalidApnsCredential":     ErrorInvalidApnsCredential,
	}
	// Occurs if push notitication message is nil.
	ErrorInvalidMessage = errors.New("message is invalid")
	// Occurs if message topic is empty.
	ErrorInvalidTarget = errors.New("topic is invalid or registration ids are not set")
	// Occurs when registration ids more than 1000.
	ErrorToManyRegIDs = errors.New("too many registrations ids")
	// Occurs if TimeToLive more than 2419200.
	ErrorInvalidTimeToLive = errors.New("messages time-to-live is invalid")
	// error if API key is not valid.
	ErrorInvalidAPIKey = errors.New("client APIKey is invalid")
	// error if registration token is not set.
	ErrorMissingRegistration = errors.New("missing registration token")
	// error if registration token is invalid.
	ErrorInvalidRegistration = errors.New("invalid registration token")
	// error when application was deleted from device and token is not registered in FCM.
	ErrorNotRegistered = errors.New("unregistered device")
	// error if package name in message is invalid.
	ErrorInvalidPackageName = errors.New("invalid package name")
	// error when application has a new registration token.
	ErrorMismatchSenderID = errors.New("mismatched sender id")
	// error when message is too big.
	ErrorMessageTooBig = errors.New("message is too big")
	// error if data key is invalid.
	ErrorInvalidDataKey = errors.New("invalid data key")
	// error when message has invalid TTL.
	ErrorInvalidTTL = errors.New("invalid time to live")
	// error when FCM service is unavailable. It makes sense to retry after this error.
	ErrorUnavailable = connectionError("timeout")
	// ErrorInternalServerError is internal FCM error. It makes sense to retry after this error.
	ErrorInternalServerError = serverError("internal server error")
	// error when client sent to many requests to the device.
	ErrorDeviceMessageRateExceeded = errors.New("device message rate exceeded")
	// error when client sent to many requests to the topics.
	ErrorTopicsMessageRateExceeded = errors.New("topics message rate exceeded")
	// error when provided parameters have the right name and type
	ErrorInvalidParameters = errors.New("check that the provided parameters have the right name and type")
	// ErrorUnknown for unknown error type
	ErrorUnknown = errors.New("unknown error type")
	// error for Invalid APNs credentials
	ErrorInvalidApnsCredential = errors.New("invalid APNs credentials")
)
