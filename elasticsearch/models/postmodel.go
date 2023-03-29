package elastic_test_model

type Post struct {
	Content          string            `json:"content"`
	Tags             []string          `json:"tags"`
	Stats            PostStats         `json:"stats"`
	Attachments      []*PostAttachment `json:"attachments"`
	Status           string            `json:"status"`
	CreatedAt        float64           `json:"created_at"`
	UpdatedAt        float64           `json:"updated_at"`
	InteractionScore int64             `json:"interaction_score"`
	Type             PostType          `json:"type"`
	Place            *PostPlace        `json:"place,omitempty"`
	Categories       []string          `json:"categories"`
}
type (
	PostStats struct {
		CommentCount int64 `json:"comment_count"`
		LikeCount    int64 `json:"like_count"`
		ViewCount    int64 `json:"view_count"`
	}
	PostPlace struct {
		Address  string    `json:"address,omitempty"`
		Name     string    `json:"name,omitempty"`
		Location *Location `json:"location,omitempty"`
	}
	PostAttachment struct {
		Content string `json:"content,omitempty"`
		URL     string `json:"url"`
	}
	Location struct {
		Lat float64 `json:"lat"`
		Lng float64 `json:"lng"`
	}
	PostType string
)

var (
	PostTypeFree     PostType = "FREE"
	PostTypeCampaign PostType = "CAMPAIGN"
)
