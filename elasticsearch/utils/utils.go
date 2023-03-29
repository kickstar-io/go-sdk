package utils

const (
	DEFAULT_SIZE = 50
	DEFAULT_FROM = 0
)

func ValidatePaging(from, size *int) {
	if *from < 0 {
		*from = DEFAULT_FROM
	}
	if *size <= 0 {
		*size = DEFAULT_SIZE
	}
}
