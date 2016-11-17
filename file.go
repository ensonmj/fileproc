package fileproc

import (
	"os"
	"path/filepath"
	"strings"
)

type FileProcessor struct {
	NumProc  int
	SplitCnt int
	Seq      bool
	LineProcessor
	FileWrapper
	fp *processor
}

func (p *FileProcessor) ProcFile(path, dir, ext string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	base := filepath.Base(path)
	noSuffix := strings.TrimSuffix(base, filepath.Ext(base))
	fw := NewFileWriter(p.FileWrapper, dir, noSuffix, ext, p.SplitCnt)
	if p.Seq {
		fw = WithSequence(fw)
	}
	return p.fp.proc(f, fw)
}

func (p *FileProcessor) ProcPath(path, dir, ext string) error {
	return filepath.Walk(path, func(subPath string, fi os.FileInfo, err error) error {
		if fi.IsDir() || err != nil {
			return nil
		}

		f, err := os.Open(subPath)
		if err != nil {
			return err
		}
		defer f.Close()

		fw := NewFileWriter(p.FileWrapper, dir, subPath, ext, p.SplitCnt)
		if p.Seq {
			fw = WithSequence(fw)
		}
		return p.fp.proc(f, fw)
	})
}

func NewFileProcessor(num, splitCnt int, seq bool, lp LineProcessor, fw FileWrapper) *FileProcessor {
	return &FileProcessor{
		NumProc:       num,
		SplitCnt:      splitCnt,
		Seq:           seq,
		LineProcessor: lp,
		FileWrapper:   fw,
		fp:            newProcessor(num, lp),
	}
}

func ProcTerm(num int, lp LineProcessor, fw FileWrapper) error {
	fp := newProcessor(num, lp)
	return fp.proc(os.Stdin, WithSequence(NewTermWriter(fw)))
}
