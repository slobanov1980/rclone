package vfscache

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/file"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWindowsOverwriteDuringActiveWriteBack(t *testing.T) {
	for _, hc := range []time.Duration{0, 5 * time.Second} {
		t.Run("HandleCaching="+hc.String(), func(t *testing.T) {
			opt := vfscommon.Opt
			opt.CachePollInterval = 0
			opt.WriteBack = fs.Duration(50 * time.Millisecond)
			opt.HandleCaching = fs.Duration(hc)

			r, c := newTestCacheOpt(t, opt)

			const targetName = "sec_master/dim_tokens.parquet"
			const tempName = "sec_master/.rclone_temp_multipart_v2"
			const v1Payload = "v1-initial-parquet-bytes"
			const v2Payload = "v2-overwritten-parquet-bytes-8677000-rows"

			// Seed v1 into cache without triggering auto-upload yet
			v1Item := c.Item(targetName)
			itemWrite(t, v1Item, v1Payload)
			v1Item.mu.Lock()
			_ = v1Item.fd.Close()
			v1Item.fd = nil
			v1Item.opens = 0
			v1Item.info.Dirty = true
			v1Item.info.Size = int64(len(v1Payload))
			require.NoError(t, v1Item._save())
			c.writeback.SetID(&v1Item.writeBackID)
			v1WbID := v1Item.writeBackID
			v1Item.mu.Unlock()

			uploadStarted := make(chan struct{})
			uploadCanceled := make(chan struct{})

			// Simulate active slow Google Drive upload holding NTFS file open via lib/file.Open
			c.writeback.Add(v1WbID, targetName, int64(len(v1Payload)), true, func(ctx context.Context) error {
				fd, openErr := file.Open(c.toOSPath(targetName))
				if openErr != nil {
					return openErr
				}
				defer func() {
					_ = fd.Close()
					close(uploadCanceled)
				}()
				close(uploadStarted)
				<-ctx.Done()
				return ctx.Err()
			})

			select {
			case <-uploadStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for v1 writeback upload to start")
			}

			// Stage v2 in temporary multipart cache item and close it (queued for writeback)
			optSlow := fs.Duration(200 * time.Millisecond)
			c.opt.WriteBack = optSlow
			v2Item := c.Item(tempName)
			itemWrite(t, v2Item, v2Payload)
			require.NoError(t, v2Item.Close(nil))

			// Atomic overwrite while v1 upload is actively holding targetName open
			require.NoError(t, c.Rename(tempName, targetName, nil))

			select {
			case <-uploadCanceled:
			case <-time.After(3 * time.Second):
				t.Fatal("expected v1 upload to be canceled before rename completed")
			}

			assertPathNotExist(t, c.toOSPath(tempName))
			assertPathNotExist(t, c.toOSPathMeta(tempName))

			diskBytes, err := os.ReadFile(c.toOSPath(targetName))
			require.NoError(t, err)
			assert.Equal(t, v2Payload, string(diskBytes))

			// Wait for v2 writeback to upload to backing Fremote and mark item clean
			require.Eventually(t, func() bool {
				q := c.writeback.Queue()
				return len(q) == 0 && !c.Item(targetName).IsDirty()
			}, 5*time.Second, 25*time.Millisecond, "expected v2 writeback queue to drain to remote")

			remoteObj, err := r.Fremote.NewObject(context.Background(), targetName)
			require.NoError(t, err)
			assert.Equal(t, int64(len(v2Payload)), remoteObj.Size())
		})
	}
}

func TestWindowsOverwriteDuringConcurrentReadAndClose(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = fs.Duration(10 * time.Second)
	opt.HandleCaching = fs.Duration(5 * time.Second)

	r, c := newTestCacheOpt(t, opt)

	const targetName = "sec_master/concurrent_read.parquet"
	const tempName = "sec_master/.rclone_temp_multipart_v3"
	const v2Payload = "v2-clean-cached-stream"
	const v3Payload = "v3-atomic-replacement-dirty-stream-longer"

	// Create remote object v2 and open a read handle in cache
	_ = r.WriteObject(context.Background(), targetName, v2Payload, time.Now())
	remoteObj, err := r.Fremote.NewObject(context.Background(), targetName)
	require.NoError(t, err)
	readerItem := c.Item(targetName)
	require.NoError(t, readerItem.Open(remoteObj))
	readBuf := make([]byte, len(v2Payload))
	n, err := readerItem.ReadAt(readBuf, 0)
	require.NoError(t, err)
	require.Equal(t, len(v2Payload), n)
	assert.Equal(t, v2Payload, string(readBuf))

	// Stage dirty v3 temp file and close it
	v3Item := c.Item(tempName)
	itemWrite(t, v3Item, v3Payload)
	require.NoError(t, v3Item.Close(nil))

	// Overwrite targetName while readerItem is STILL open (opens == 1)
	require.NoError(t, c.Rename(tempName, targetName, nil))

	// Open reader continues reading v2 from its unlinked handle, then closes
	readBuf2 := make([]byte, len(v2Payload))
	n2, err := readerItem.ReadAt(readBuf2, 0)
	require.NoError(t, err)
	assert.Equal(t, v2Payload, string(readBuf2[:n2]))
	require.NoError(t, readerItem.Close(nil))

	// Verify readerItem.Close() did NOT overwrite v3's vfsMeta with Dirty:false or v2's size
	metaBytes, err := os.ReadFile(c.toOSPathMeta(targetName))
	require.NoError(t, err)
	var metaInfo Info
	require.NoError(t, json.Unmarshal(metaBytes, &metaInfo))
	assert.True(t, metaInfo.Dirty, "closing superseded reader must not clear v3 Dirty flag in vfsMeta")
	assert.Equal(t, int64(len(v3Payload)), metaInfo.Size)

	currentBytes, err := os.ReadFile(c.toOSPath(targetName))
	require.NoError(t, err)
	assert.Equal(t, v3Payload, string(currentBytes))

	// Cancel deferred 10s writeback before test cleanup
	c.Item(targetName).remove("test cleanup")
}

func TestRenameErrorPropagatesAndPreservesDestination(t *testing.T) {
	opt := vfscommon.Opt
	opt.CachePollInterval = 0
	opt.WriteBack = fs.Duration(10 * time.Second)
	opt.HandleCaching = 0

	_, c := newTestCacheOpt(t, opt)

	const dirBlockedTarget = "sec_master/blocked_target"
	const tempName = "sec_master/.rclone_temp_fail"
	const tempPayload = "temp-upload-bytes"

	srcItem := c.Item(tempName)
	itemWrite(t, srcItem, tempPayload)
	require.NoError(t, srcItem.Close(nil))

	// Make destination OS path a directory so rename() deterministically fails on both Windows and Unix
	require.NoError(t, os.MkdirAll(c.toOSPath(dirBlockedTarget), 0o755))

	err := c.Rename(tempName, dirBlockedTarget, nil)
	require.Error(t, err)

	// Source item must retain its original name and files without corrupting cache state
	assert.Equal(t, tempName, srcItem.GetName())
	stillThere, readErr := os.ReadFile(c.toOSPath(tempName))
	require.NoError(t, readErr)
	assert.Equal(t, tempPayload, string(stillThere))
	_ = runtime.GOOS

	require.NoError(t, os.RemoveAll(c.toOSPath(dirBlockedTarget)))
	srcItem.remove("test cleanup")
}
