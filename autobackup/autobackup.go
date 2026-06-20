package autobackup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const (
	backupFilePrefix = "rqlite-backup-"
	backupFileExt    = ".db"
	defaultKeepCount = 7
)

type BackupProvider interface {
	Backup(w io.Writer) error
}

type Service struct {
	provider       BackupProvider
	dir            string
	interval       time.Duration
	keepCount      int
	logger         logger
	done           chan struct{}
	lastBackup     time.Time
	lastBackupSize int64
}

type logger interface {
	Printf(format string, v ...any)
	Println(v ...any)
}

type defaultLogger struct{}

func (l *defaultLogger) Printf(format string, v ...any) {
	fmt.Printf("[autobackup] "+format+"\n", v...)
}

func (l *defaultLogger) Println(v ...any) {
	fmt.Printf("[autobackup] %v\n", v)
}

func New(provider BackupProvider, dir string, interval time.Duration, keepCount int) *Service {
	if keepCount <= 0 {
		keepCount = defaultKeepCount
	}
	return &Service{
		provider:  provider,
		dir:       dir,
		interval:  interval,
		keepCount: keepCount,
		logger:    &defaultLogger{},
		done:      make(chan struct{}),
	}
}

func (s *Service) SetLogger(l logger) {
	s.logger = l
}

func (s *Service) Start(ctx context.Context) {
	s.logger.Printf("starting auto backup to %s every %s, keep last %d", s.dir, s.interval, s.keepCount)

	ticker := time.NewTicker(s.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				s.logger.Println("auto backup service shutting down")
				return
			case <-s.done:
				return
			case <-ticker.C:
				if err := s.backup(); err != nil {
					s.logger.Printf("auto backup failed: %v", err)
				}
			}
		}
	}()
}

func (s *Service) Stop() {
	close(s.done)
}

func (s *Service) backup() error {
	if err := os.MkdirAll(s.dir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	timestamp := time.Now().Format("20060102-150405")
	filename := fmt.Sprintf("%s%s%s", backupFilePrefix, timestamp, backupFileExt)
	fullPath := filepath.Join(s.dir, filename)

	tmpPath := fullPath + ".tmp"
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("failed to create temp backup file: %w", err)
	}

	if err := s.provider.Backup(f); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("failed to perform backup: %w", err)
	}

	fi, err := f.Stat()
	if err == nil {
		s.lastBackupSize = fi.Size()
	}
	f.Close()

	if err := os.Rename(tmpPath, fullPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to finalize backup file: %w", err)
	}

	s.lastBackup = time.Now()
	s.logger.Printf("auto backup completed: %s (%d bytes)", filename, s.lastBackupSize)

	if err := s.cleanupOld(); err != nil {
		s.logger.Printf("failed to cleanup old backups: %v", err)
	}

	return nil
}

func (s *Service) cleanupOld() error {
	files, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}

	var backupFiles []string
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		name := f.Name()
		if strings.HasPrefix(name, backupFilePrefix) && strings.HasSuffix(name, backupFileExt) {
			backupFiles = append(backupFiles, name)
		}
	}

	if len(backupFiles) <= s.keepCount {
		return nil
	}

	sort.Strings(backupFiles)

	toRemove := backupFiles[:len(backupFiles)-s.keepCount]
	for _, f := range toRemove {
		fp := filepath.Join(s.dir, f)
		if err := os.Remove(fp); err != nil {
			s.logger.Printf("failed to remove old backup %s: %v", f, err)
		}
	}

	return nil
}

func (s *Service) Stats() (map[string]any, error) {
	return map[string]any{
		"dir":              s.dir,
		"interval":         s.interval.String(),
		"keep_count":       s.keepCount,
		"last_backup":      s.lastBackup.Format(time.RFC3339),
		"last_backup_size": s.lastBackupSize,
	}, nil
}

func (s *Service) LastBackup() time.Time {
	return s.lastBackup
}
