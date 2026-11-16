//go:build windows

package vfscache

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/windows"
)

// osRenameReplace renames osOldPath over osNewPath on Windows using os.Root.Rename,
// which invokes NtSetInformationFile(FileRenameInformationEx) with
// FILE_RENAME_REPLACE_IF_EXISTS | FILE_RENAME_POSIX_SEMANTICS so open readers holding
// FILE_SHARE_DELETE handles do not fail atomic VFS cache overwrites with Access is denied.
func osRenameReplace(osOldPath, osNewPath string) error {
	backoffs := [...]time.Duration{
		25 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
	}

	var lastErr error
	for attempt := 0; attempt <= len(backoffs); attempt++ {
		lastErr = tryPosixRootRename(osOldPath, osNewPath)
		if lastErr == nil {
			return nil
		}
		if !isTransientWindowsSharingError(lastErr) || attempt == len(backoffs) {
			return lastErr
		}
		time.Sleep(backoffs[attempt])
	}
	return lastErr
}

func tryPosixRootRename(osOldPath, osNewPath string) error {
	absOld, errOld := filepath.Abs(osOldPath)
	absNew, errNew := filepath.Abs(osNewPath)
	if errOld == nil && errNew == nil {
		if rootDir, relOld, relNew, ok := splitCommonRoot(absOld, absNew); ok {
			root, err := os.OpenRoot(rootDir)
			if err == nil {
				renameErr := root.Rename(relOld, relNew)
				_ = root.Close()
				if renameErr == nil {
					return nil
				}
				// Fall back to os.Rename only if the underlying filesystem rejects Root.Rename
				// with invalid parameter / unsupported function; preserve real sharing/ENOENT errors.
				if !errors.Is(renameErr, windows.ERROR_INVALID_FUNCTION) &&
					!errors.Is(renameErr, windows.ERROR_INVALID_PARAMETER) &&
					!errors.Is(renameErr, windows.ERROR_NOT_SUPPORTED) {
					return renameErr
				}
			}
		}
	}
	return os.Rename(osOldPath, osNewPath)
}

func splitCommonRoot(absOld, absNew string) (rootDir, relOld, relNew string, ok bool) {
	volOld := filepath.VolumeName(absOld)
	volNew := filepath.VolumeName(absNew)
	if volOld == "" || !strings.EqualFold(volOld, volNew) {
		return "", "", "", false
	}

	oldDir := filepath.Dir(absOld)
	newDir := filepath.Dir(absNew)
	if strings.EqualFold(oldDir, newDir) {
		return oldDir, filepath.Base(absOld), filepath.Base(absNew), true
	}

	oldParts := strings.Split(filepath.Clean(absOld[len(volOld):]), string(filepath.Separator))
	newParts := strings.Split(filepath.Clean(absNew[len(volNew):]), string(filepath.Separator))
	common := 0
	maxCommon := min(len(oldParts)-1, len(newParts)-1)
	for common < maxCommon && strings.EqualFold(oldParts[common], newParts[common]) {
		common++
	}
	prefix := volOld + string(filepath.Separator) + filepath.Join(oldParts[:common]...)
	relOldPath, err1 := filepath.Rel(prefix, absOld)
	relNewPath, err2 := filepath.Rel(prefix, absNew)
	if err1 != nil || err2 != nil || strings.HasPrefix(relOldPath, "..") || strings.HasPrefix(relNewPath, "..") {
		return "", "", "", false
	}
	return prefix, relOldPath, relNewPath, true
}

func isTransientWindowsSharingError(err error) bool {
	return errors.Is(err, windows.ERROR_ACCESS_DENIED) ||
		errors.Is(err, windows.ERROR_SHARING_VIOLATION) ||
		errors.Is(err, windows.ERROR_LOCK_VIOLATION)
}
