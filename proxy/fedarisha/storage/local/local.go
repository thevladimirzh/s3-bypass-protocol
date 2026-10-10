// Package local implements the Storage interface using the local filesystem.
// Use it for testing or with cloud-sync clients (Yandex Disk desktop app, etc.).
package local

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/xtls/xray-core/proxy/fedarisha/storage"
)

// uploadTempPrefix marks a file that is being written and is not yet an
// object. Chosen so it cannot collide with a session file: real objects are
// named s_%08x / c_%08x.
const uploadTempPrefix = ".upload-"

type Config struct {
	RootDir string `json:"root_dir"`
}

type Local struct {
	root string
}

func New(cfg Config) *Local {
	return &Local{root: cfg.RootDir}
}

func (l *Local) Init(_ context.Context) error {
	return os.MkdirAll(l.root, 0o755)
}

func (l *Local) EnsureDir(_ context.Context, path string) error {
	fp, err := l.abs(path)
	if err != nil {
		return err
	}
	return os.MkdirAll(fp, 0o755)
}

// Upload writes the object through a temporary file and renames it into place.
//
// Writing in place would truncate first and write second, and the protocol's
// reader is List-then-GET with no size check — so it could read a half-written
// object, exactly as it cannot against S3, where an object appears only once
// its body is complete. Rename is atomic on POSIX and on Windows, so a reader
// sees either the whole previous object or the whole new one.
//
// The temporary name includes a suffix that cannot collide with a real object,
// because a stray temp file appearing in a listing would be handed to the peer
// as if it were session data.
func (l *Local) Upload(_ context.Context, path string, data []byte) error {
	fp, err := l.abs(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(fp), 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(fp), ".upload-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		// Best effort: the rename has already consumed it on success.
		_ = os.Remove(tmpName)
	}()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpName, fp)
}

func (l *Local) Download(_ context.Context, path string) ([]byte, error) {
	fp, err := l.abs(path)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(fp)
}

func (l *Local) List(_ context.Context, dir string, prefix string) ([]storage.FileInfo, error) {
	dirPath, err := l.abs(dir)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var result []storage.FileInfo
	for _, e := range entries {
		// A partially written upload is not an object yet. S3 never lists one,
		// so listing one here would hand the peer a file that is not there.
		if strings.HasPrefix(e.Name(), uploadTempPrefix) {
			continue
		}
		if prefix != "" && !strings.HasPrefix(e.Name(), prefix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		result = append(result, storage.FileInfo{
			Name:     e.Name(),
			Path:     filepath.Join(dir, e.Name()),
			Size:     info.Size(),
			Created:  info.ModTime(), // POSIX has no birth time — use mtime.
			Modified: info.ModTime(),
			IsDir:    e.IsDir(),
		})
	}
	return result, nil
}

func (l *Local) Delete(_ context.Context, path string) error {
	fp, err := l.abs(path)
	if err != nil {
		return err
	}
	if err := os.Remove(fp); os.IsNotExist(err) {
		return nil
	} else {
		return err
	}
}

func (l *Local) Watch(ctx context.Context, dir string, since time.Time, timeout time.Duration) ([]storage.FileInfo, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		files, err := l.List(ctx, dir, "")
		if err != nil {
			return nil, err
		}
		var newer []storage.FileInfo
		for _, f := range files {
			if f.Modified.After(since) {
				newer = append(newer, f)
			}
		}
		if len(newer) > 0 {
			return newer, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return nil, nil
}

// abs resolves a storage-relative path and refuses anything that escapes the
// root.
//
// The containment test requires a separator after the root, not merely the
// root as a string prefix: "/var/fed" is a prefix of "/var/fed-backup", so a
// sibling directory with a common name would otherwise pass.
//
// A refusal is an error rather than a panic. Every method routes through here,
// so panicking took the process down on the caller's goroutine instead of
// surfacing where it could be logged and handled.
func (l *Local) abs(rel string) (string, error) {
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" {
		return l.root, nil
	}
	fp := filepath.Join(l.root, filepath.FromSlash(rel))

	abs, err := filepath.Abs(fp)
	if err != nil {
		return "", fmt.Errorf("resolve %s: %w", rel, err)
	}
	rootAbs, err := filepath.Abs(l.root)
	if err != nil {
		return "", fmt.Errorf("resolve root: %w", err)
	}
	if abs != rootAbs && !strings.HasPrefix(abs, rootAbs+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q escapes the storage root", rel)
	}
	return abs, nil
}
