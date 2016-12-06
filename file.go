package fileproc

import (
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FileProcessor struct {
	SplitCnt   int
	Seq        bool
	PrefixTime bool
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
	if p.PrefixTime {
		day := time.Now().Format("20060102150405")
		noSuffix = day + "_" + noSuffix
	}
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

		base := filepath.Base(subPath)
		noSuffix := strings.TrimSuffix(base, filepath.Ext(base))
		if p.PrefixTime {
			day := time.Now().Format("20060102150405")
			noSuffix = day + "_" + noSuffix
		}
		fw := NewFileWriter(p.FileWrapper, dir, noSuffix, ext, p.SplitCnt)
		if p.Seq {
			fw = WithSequence(fw)
		}
		return p.fp.run(f, fw)
	})
}

func (p *FileProcessor) ProcPathReverse(path, dir, ext string) error {
	var fns []string
	filepath.Walk(path, func(subPath string, fi os.FileInfo, err error) error {
		if fi.IsDir() || err != nil {
			return nil
		}
		fns = append(fns, subPath)
		return nil
	})

	num := len(fns)
	for i := num - 1; i >= 0; i-- {
		fn := fns[i]
		f, err := os.Open(fn)
		if err != nil {
			return err
		}
		defer f.Close()

		base := filepath.Base(fn)
		noSuffix := strings.TrimSuffix(base, filepath.Ext(base))
		if p.PrefixTime {
			day := time.Now().Format("20060102150405")
			noSuffix = day + "_" + noSuffix
		}
		fw := NewFileWriter(p.FileWrapper, dir, noSuffix, ext, p.SplitCnt)
		if p.Seq {
			fw = WithSequence(fw)
		}
		err = p.fp.run(f, fw)
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *FileProcessor) Stat() (int, int, int) {
	return p.fp.stat()
}

func NewFileProcessor(num, splitCnt int, seq, prefixTime bool, m Mapper, r Reducer, fw FileWrapper) *FileProcessor {
	return &FileProcessor{
		SplitCnt:    splitCnt,
		Seq:         seq,
		PrefixTime:  prefixTime,
		FileWrapper: fw,
		fp:          newProcessor(num, m, r),
	}
}

func ProcTerm(num int, m Mapper, r Reducer, fw FileWrapper) error {
	fp := newProcessor(num, m, r)
	return fp.run(os.Stdin, WithSequence(NewTermWriter(fw)))
}
