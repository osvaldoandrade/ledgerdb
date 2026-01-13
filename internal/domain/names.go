package domain

import "strings"

func IsValidCollectionName(name string) bool {
	if strings.ContainsAny(name, "/\\") {
		return false
	}
	if strings.Contains(name, "..") {
		return false
	}
	return true
}
