package firebase

import (
	"strings"
)

// Notification payload.
type Notification struct {
	Title        string `json:"title,omitempty"`
	Body         string `json:"body,omitempty"`
	ChannelID    string `json:"android_channel_id,omitempty"`
	Icon         string `json:"icon,omitempty"`
	Image        string `json:"image,omitempty"`
	Sound        string `json:"sound,omitempty"`
	Badge        string `json:"badge,omitempty"`
	Tag          string `json:"tag,omitempty"`
	Color        string `json:"color,omitempty"`
	ClickAction  string `json:"click_action,omitempty"`
	BodyLocKey   string `json:"body_loc_key,omitempty"`
	BodyLocArgs  string `json:"body_loc_args,omitempty"`
	TitleLocKey  string `json:"title_loc_key,omitempty"`
	TitleLocArgs string `json:"title_loc_args,omitempty"`
}

//Targets, options, and payload for messages.
type Message struct {
	To                    string                 `json:"to,omitempty"`
	RegistrationIDs       []string               `json:"registration_ids,omitempty"`
	Condition             string                 `json:"condition,omitempty"`
	CollapseKey           string                 `json:"collapse_key,omitempty"`
	Priority              string                 `json:"priority,omitempty"`
	ContentAvailable      bool                   `json:"content_available,omitempty"`
	MutableContent        bool                   `json:"mutable_content,omitempty"`
	TimeToLive            *uint                  `json:"time_to_live,omitempty"`
	DryRun                bool                   `json:"dry_run,omitempty"`
	RestrictedPackageName string                 `json:"restricted_package_name,omitempty"`
	Notification          *Notification          `json:"notification,omitempty"`
	Data                  map[string]interface{} `json:"data,omitempty"`
	Apns                  map[string]interface{} `json:"apns,omitempty"`
	Webpush               map[string]interface{} `json:"webpush,omitempty"`
}

// Validate returns an error if the message is not well-formed.
func (msg *Message) Validate() error {
	if msg == nil {
		return ErrorInvalidMessage
	}
	// validate target identifier: `to` or `condition`, or `registration_ids`
	opCnt := strings.Count(msg.Condition, "&&") + strings.Count(msg.Condition, "||")
	if msg.To == "" && (msg.Condition == "" || opCnt > 5) && len(msg.RegistrationIDs) == 0 {
		return ErrorInvalidTarget
	}
	if len(msg.RegistrationIDs) > 1000 {
		return ErrorToManyRegIDs
	}
	if msg.TimeToLive != nil && *msg.TimeToLive > uint(2419200) {
		return ErrorInvalidTimeToLive
	}
	return nil
}
