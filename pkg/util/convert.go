package util

func SliceToMap[T comparable](slice []T) map[T]struct{} {
	result := make(map[T]struct{})
	for _, v := range slice {
		result[v] = struct{}{}
	}
	return result
}
