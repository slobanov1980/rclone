//go:build !windows

package vfscache

import "os"

func osRenameReplace(osOldPath, osNewPath string) error {
	return os.Rename(osOldPath, osNewPath)
}
