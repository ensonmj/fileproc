package fileproc

import (
	"os"
	"path/filepath"
	"strings"
)

type FileProcessor struct {
	SplitCnt int
	Seq      bool
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
	return p.fp.run(f, fw)
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
		return p.fp.run(f, fw)
	})
}

func (p *FileProcessor) Stat() (int, int, int, int) {
	return p.fp.stat()
}

func NewFileProcessor(num, splitCnt int, seq bool, m Mapper, r Reducer, fw FileWrapper) *FileProcessor {
	return &FileProcessor{
		SplitCnt:    splitCnt,
		Seq:         seq,
		FileWrapper: fw,
		fp:          newProcessor(num, m, r),
	}
}

func ProcTerm(num int, m Mapper, r Reducer, fw FileWrapper) error {
	fp := newProcessor(num, m, r)
	return fp.run(os.Stdin, WithSequence(NewTermWriter(fw)))
}
