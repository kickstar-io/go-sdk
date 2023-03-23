package utils

import (
	"fmt"
	"testing"
)

type IDField struct {
	ID interface{} `json:"id" bson:"_id,omitempty"`
}

type DefaultModel struct {
	IDField `bson:",inline"`
	//DateFields `bson:",inline"`
}

type Users struct {
	DefaultModel       `json:",inline" bson:",inline"`
	Phone              string                 `json:"phone,omitempty" bson:"phone,omitempty"`
	FullName           string                 `json:"fullname,omitempty" bson:"fullname,omitempty"`
	Username           string                 `json:"username,omitempty" bson:"username,omitempty"`
	Email              string                 `json:"email,omitempty" bson:"email,omitempty"`
	HomeAddress        string                 `json:"home_address,omitempty" bson:"home_address,omitempty"`
	AddressComponents  map[string]interface{} `json:"address_components,omitempty" bson:"address_components,omitempty"`
	Avatar             string                 `json:"avatar,omitempty" bson:"avatar,omitempty"`
	CoverPhotoURL      string                 `json:"cover_photo_url,omitempty" bson:"cover_photo_url,omitempty"`
	CountryCode        string                 `json:"country_code,omitempty" bson:"country_code,omitempty"`
	Status             string                 `json:"status,omitempty" bson:"status,omitempty"`
	CreatedAt          float64                `json:"created_at,omitempty" bson:"created_at,omitempty"`
	WalletAddress      string                 `json:"wallet_address,omitempty" bson:"wallet_address,omitempty"`
	UpdatedAt          float64                `json:"updated_at,omitempty" bson:"updated_at,omitempty"`
	Source             string                 `json:"source,omitempty" bson:"source,omitempty"`
	IsDeleted          bool                   `json:"is_deleted,omitempty" bson:"is_deleted,omitempty"`
	IsVerified         bool                   `json:"is_verified" bson:"is_verified"`
	ActivateTime       float64                `json:"activate_time,omitempty" bson:"activate_time,omitempty"`
	Language           string                 `json:"language,omitempty" bson:"language,omitempty"`
	InterestCategories []string               `json:"interestCategories" bson:"interestCategories"`
	Child              Child                  `json:"child,omitempty" bson:"child,omitempty"`
}

type Child struct {
	ActivateTime float64 `json:"activate_time,omitempty" bson:"activate_time,omitempty"`
	Language     string  `json:"language,omitempty" bson:"language,omitempty"`
}

func TestParseBsonMap(t *testing.T) {
	user := &Users{
		Phone:    "aaa",
		FullName: "aaa",
		Username: "aaa",
		Child: Child{
			ActivateTime: 1,
			Language:     "2",
		},
	}

	userMap := ConvertStructToBSONMap(user, nil)

	fmt.Printf("%+v\n", userMap)
}
