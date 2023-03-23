package firebase

import (
	"encoding/json"
)

// connection errors such as timeout error
type connectionError string

func (err connectionError) Error() string {
	return string(err)
}
func (err connectionError) Temporary() bool {
	return true
}
func (err connectionError) Timeout() bool {
	return true
}

// internal server errors.
type serverError string

func (err serverError) Error() string {
	return string(err)
}

func (serverError) Temporary() bool {
	return true
}

func (serverError) Timeout() bool {
	return false
}

// Response represents the FCM server's response to the application server's sent message.
type Response struct {
	MulticastID  int64    `json:"multicast_id"`
	Success      int      `json:"success"`
	Failure      int      `json:"failure"`
	CanonicalIDs int      `json:"canonical_ids"`
	Results      []Result `json:"results"`
	// Device Group HTTP Response
	FailedRegistrationIDs []string `json:"failed_registration_ids"`
	// Topic HTTP response
	MessageID         int64 `json:"message_id"`
	Error             error `json:"error"`
	ErrorResponseCode string
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (r *Response) UnmarshalJSON(data []byte) error {
	var response struct {
		MulticastID  int64    `json:"multicast_id"`
		Success      int      `json:"success"`
		Failure      int      `json:"failure"`
		CanonicalIDs int      `json:"canonical_ids"`
		Results      []Result `json:"results"`
		// Device Group HTTP Response
		FailedRegistrationIDs []string `json:"failed_registration_ids"`
		// Topic HTTP response
		MessageID int64  `json:"message_id"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return err
	}
	r.MulticastID = response.MulticastID
	r.Success = response.Success
	r.Failure = response.Failure
	r.CanonicalIDs = response.CanonicalIDs
	r.Results = response.Results
	r.FailedRegistrationIDs = response.FailedRegistrationIDs
	r.MessageID = response.MessageID
	r.ErrorResponseCode = response.Error
	if response.Error != "" {
		if val, ok := errMap[response.Error]; ok {
			r.Error = val
		} else {
			r.Error = ErrorUnknown
		}
	}
	return nil
}

// Result of a processed message.
type Result struct {
	MessageID         string `json:"message_id"`
	RegistrationID    string `json:"registration_id"`
	Error             error  `json:"error"`
	ErrorResponseCode string
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (r *Result) UnmarshalJSON(data []byte) error {
	var result struct {
		MessageID      string `json:"message_id"`
		RegistrationID string `json:"registration_id"`
		Error          string `json:"error"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return err
	}
	r.MessageID = result.MessageID
	r.RegistrationID = result.RegistrationID
	r.ErrorResponseCode = result.Error
	if result.Error != "" {
		if val, ok := errMap[result.Error]; ok {
			r.Error = val
		} else {
			r.Error = ErrorUnknown
		}
	}
	return nil
}

// Checks if the device token is unregistered, according to response from FCM server. Useful to determineif app is uninstalled.
func (r Result) Unregistered() bool {
	switch r.Error {
	case ErrorNotRegistered, ErrorMismatchSenderID, ErrorMissingRegistration, ErrorInvalidRegistration:
		return true
	default:
		return false
	}
}
