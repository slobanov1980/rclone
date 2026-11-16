package vfs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/rc"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileRenameCacheFailureReturnsErrorAndPreservesLeaf(t *testing.T) {
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeFull
	opt.WriteBack = fs.Duration(10 * time.Second)
	_, vfs := newTestVFSOpt(t, &opt)

	const tempPath = "dir/.rclone_temp_multipart_123"
	const targetPath = "dir/blocked_target"

	require.NoError(t, vfs.Mkdir("dir", 0o777))
	fh, err := vfs.OpenFile(tempPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	require.NoError(t, err)
	_, err = fh.Write([]byte("multipart-body-bytes"))
	require.NoError(t, err)
	require.NoError(t, fh.Close())

	// Block the target OS cache path with a directory so Cache.Rename fails deterministically
	stats := vfs.Stats()
	diskCache := stats["diskCache"].(rc.Params)
	cacheRoot := diskCache["path"].(string)
	blockedOSDir := filepath.Join(cacheRoot, filepath.FromSlash(targetPath))
	require.NoError(t, os.MkdirAll(blockedOSDir, 0o755))
	defer func() { _ = os.RemoveAll(blockedOSDir) }()

	err = vfs.Rename(tempPath, targetPath)
	require.Error(t, err, "File.rename must propagate VFS cache rename errors (#9943)")

	// Original node must remain addressable at tempPath with unmutated leaf (#10059)
	node, statErr := vfs.Stat(tempPath)
	require.NoError(t, statErr)
	assert.Equal(t, ".rclone_temp_multipart_123", node.Name())

	_, targetStatErr := vfs.Stat(targetPath)
	assert.ErrorIs(t, targetStatErr, ENOENT)

	require.NoError(t, os.RemoveAll(blockedOSDir))
	require.NoError(t, node.Remove())
}
