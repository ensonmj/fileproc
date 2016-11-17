package fileproc

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type FileWrapper interface {
	BeforeWrite(*os.File) error
	AfterWrite(*os.File) error
}

type dummyWrapper int

func (w *dummyWrapper) BeforeWrite(*os.File) error {
	return nil
}

func (w *dummyWrapper) AfterWrite(*os.File) error {
	return nil
}

var dummy = new(dummyWrapper)

func DummyWrapper() FileWrapper {
	return dummy
}

type FileWriter interface {
	Open() error
	Write(li lineInfo) (int, error)
	Close() error
}

type TermWriter struct {
	FileWrapper
}

func (w *TermWriter) Open() error {
	return w.BeforeWrite(os.Stdout)
}

func (w *TermWriter) Write(li lineInfo) (int, error) {
	return os.Stdout.Write(li.Bytes)
}

func (w *TermWriter) Close() error {
	return w.AfterWrite(os.Stdout)
}

func NewTermWriter(fw FileWrapper) *TermWriter {
	return &TermWriter{FileWrapper: fw}
}

type simpleWriter struct {
	FileWrapper
	fn   string
	dir  string
	ext  string
	file *os.File
}

func (w *simpleWriter) Open() error {
	path := filepath.Join(w.dir, w.fn+w.ext)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w.file = f
	w.BeforeWrite(w.file)
	return nil
}

func (w *simpleWriter) Write(li lineInfo) (int, error) {
	return w.file.Write(li.Bytes)
}

func (w *simpleWriter) Close() error {
	w.AfterWrite(w.file)
	return w.file.Close()
}

type splitWriter struct {
	simpleWriter
	splitCnt     int
	writeLineCnt int
	index        int
}

func (w *splitWriter) Write(li lineInfo) (int, error) {
	if li.Bytes == nil {
		return 0, nil
	}

	if w.file == nil {
		w.index = w.writeLineCnt / w.splitCnt
		path := filepath.Join(w.dir, w.fn+"_"+strconv.Itoa(w.index)+w.ext)
		f, err := os.Create(path)
		if err != nil {
			return 0, err
		}
		w.file = f
		w.BeforeWrite(w.file)
	}

	n, err := w.file.Write(li.Bytes)
	if w.writeLineCnt%w.splitCnt == w.splitCnt-1 && w.file != nil {
		w.AfterWrite(w.file)
		w.file.Close()
		w.file = nil
	}
	if err != nil {
		return 0, err
	}

	w.writeLineCnt++
	return n, nil
}

func (w *splitWriter) Close() error {
	w.AfterWrite(w.file)
	err := w.file.Close()
	index := w.index
	for {
		index++
		path := filepath.Join(w.dir, w.fn+"_"+strconv.Itoa(index)+w.ext)
		if err := os.Remove(path); os.IsNotExist(err) {
			break
		}
	}
	return err
}

func NewFileWriter(fw FileWrapper, dir, fn, ext string, splitCnt int) FileWriter {
	base := filepath.Base(fn)
	noSuffix := strings.TrimSuffix(base, filepath.Ext(base))
	w := simpleWriter{
		FileWrapper: fw,
		dir:         dir,
		fn:          noSuffix,
		ext:         ext,
	}
	if splitCnt <= 0 {
		return &w
	}

	return &splitWriter{
		simpleWriter: w,
		splitCnt:     splitCnt,
	}
}

type LineInfoSlice []lineInfo

func (lis LineInfoSlice) Len() int { return len(lis) }

func (lis LineInfoSlice) Swap(i, j int) { lis[i], lis[j] = lis[j], lis[i] }

// ascending order
func (lis LineInfoSlice) Less(i, j int) bool { return lis[i].Index < lis[j].Index }

type seqFileWriter struct {
	FileWriter
	cache     LineInfoSlice
	currIndex int
}

func (w *seqFileWriter) Write(li lineInfo) (int, error) {
	if li.Index != w.currIndex {
		w.cache = append(w.cache, li)
		sort.Sort(w.cache)
		return 0, nil
	}
	w.currIndex++

	n, err := w.FileWriter.Write(li)
	if err != nil {
		return n, err
	}
	totalNum := n

	// read from cache
	cacheIndex := 0
	for _, li := range w.cache {
		if li.Index != w.currIndex {
			break
		}
		cacheIndex++
		w.currIndex++

		n, err := w.FileWriter.Write(li)
		totalNum += n
		if err != nil {
			return totalNum, err
		}
	}
	if cacheIndex > 0 {
		w.cache = w.cache[:copy(w.cache, w.cache[cacheIndex:])]
	}

	return totalNum, nil
}

// WithSequence keep output follow input sequence
func WithSequence(parent FileWriter) *seqFileWriter {
	return &seqFileWriter{
		FileWriter: parent,
	}
}
