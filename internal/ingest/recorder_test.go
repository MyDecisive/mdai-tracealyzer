package ingest

import "github.com/mydecisive/mdai-tracealyzer/internal/buffer"

var _ Recorder = (*buffer.ValkeyBuffer)(nil)
