package tenderseed

import "os"

// MkdirAll creates a directory and all its parents if they do not exist.
func MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}
