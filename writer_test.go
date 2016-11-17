package fileproc

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

type bufferWriter struct {
	buf bytes.Buffer
}

func (w *bufferWriter) Open() error {
	return nil
}

func (w *bufferWriter) Write(li lineInfo) (int, error) {
	return w.buf.Write(li.Bytes)
}

func (w *bufferWriter) Close() error {
	return nil
}

func newBufferWriter() *bufferWriter {
	return &bufferWriter{}
}

func TestSequenceWrite(t *testing.T) {
	var buf bytes.Buffer
	inNum := 1000
	inSlice := randByteSlice(inNum, 20)
	for _, b := range inSlice {
		buf.Write(b)
		buf.Write([]byte{'\n'})
	}

	lw := &LineWorker{}
	bw := newBufferWriter()
	fp := newProcessor(10, lw)
	fp.proc(bufio.NewReader(&buf), WithSequence(bw))

	outSlice := bytes.Split(bw.buf.Bytes(), []byte{'\n'})
	outSlice = outSlice[:len(outSlice)-1]
	outNum := len(outSlice)
	if outNum != inNum {
		t.Fatalf("Expected %v, but got %v", inNum, outNum)
	} else {
		t.Logf("Expected %v", inNum)
	}
	for i, out := range outSlice {
		in := inSlice[i]
		if string(out) != string(in) {
			t.Fatalf("Expected %v, but got %v", in, out)
		} else {
			t.Logf("Expected %v", out)
		}
	}
}

func TestSplitWrite(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "fileproc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	file, err := ioutil.TempFile(dir, "fileproc")
	if err != nil {
		t.Fatal(err)
	}

	inNum := 1000
	inSlice := randByteSlice(inNum, 20)
	for _, b := range inSlice {
		file.Write(b)
		file.Write([]byte{'\n'})
	}

	lw := &LineWorker{}
	fp := NewFileProcessor(10, 100, true, lw, DummyWrapper())
	fp.ProcPath(dir, dir, ".out")

	index := 0
	var fn string
	for i := 0; i < 10; i++ {
		if i > 0 {
			fn = file.Name() + "_" + strconv.Itoa(i) + ".out"
		} else {
			fn = file.Name() + ".out"
		}
		of, err := os.Open(fn)
		if err != nil {
			t.Fatal(err)
		}
		defer of.Close()
		sc := bufio.NewScanner(of)
		for sc.Scan() {
			in := string(inSlice[index])
			out := sc.Text()
			if out != in {
				t.Fatalf("Expected %v, but got %v", in, out)
			} else {
				t.Logf("Expected %v", in)
			}
			index++
		}
	}
}

func BenchmarkWriter(b *testing.B) {
	var buf bytes.Buffer
	inNum := 1000
	inSlice := randByteSlice(inNum, 20)
	for _, b := range inSlice {
		buf.Write(b)
		buf.Write([]byte{'\n'})
	}

	lw := &LineWorker{}
	fp := newProcessor(10, lw)
	bw := newBufferWriter()

	rand.Seed(9893489983248324)
	for i := 0; i < b.N; i++ {
		fp.proc(bufio.NewReader(&buf), bw)
	}
}

func BenchmarkSeqWriter(b *testing.B) {
	var buf bytes.Buffer
	inNum := 1000
	inSlice := randByteSlice(inNum, 20)
	for _, b := range inSlice {
		buf.Write(b)
		buf.Write([]byte{'\n'})
	}

	lw := &LineWorker{}
	fp := newProcessor(10, lw)
	bw := newBufferWriter()

	rand.Seed(9893489983248324)
	for i := 0; i < b.N; i++ {
		fp.proc(bufio.NewReader(&buf), WithSequence(bw))
	}
}
