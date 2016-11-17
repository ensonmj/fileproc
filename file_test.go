package fileproc

import (
	"bufio"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
)

type LineWorker struct {
}

func (w *LineWorker) Process(line []byte) []byte {
	return append(line, '\n')
}

func randBytes(maxLen int) []byte {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	length := rand.Int()%maxLen + 1 // make sure length > 0
	buf := make([]byte, length)
	for i := 0; i < length; i++ {
		buf[i] = alphanum[rand.Int63()%int64(len(alphanum))]
	}
	return buf
}

func randByteSlice(num, maxLen int) [][]byte {
	rand.Seed(9893489983248324)
	var byteSlice [][]byte
	for i := 0; i < num; i++ {
		byteSlice = append(byteSlice, randBytes(maxLen))
	}
	return byteSlice
}

func TestPathProc(t *testing.T) {
	dir, err := ioutil.TempDir(os.TempDir(), "fileproc")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	file1, err := ioutil.TempFile(dir, "fileproc")
	if err != nil {
		t.Fatal(err)
	}
	file2, err := ioutil.TempFile(dir, "fileproc")
	if err != nil {
		t.Fatal(err)
	}

	inNum := 1000
	inSlice := randByteSlice(inNum, 20)
	for _, b := range inSlice {
		file1.Write(b)
		file1.Write([]byte{'\n'})

		file2.Write(b)
		file2.Write([]byte{'\n'})
	}

	lw := &LineWorker{}
	fp := NewFileProcessor(10, 0, true, lw, DummyWrapper())
	fp.ProcPath(dir, dir, ".out")

	of1, err := os.Open(file1.Name() + ".out")
	if err != nil {
		t.Fatal(err)
	}
	defer of1.Close()
	sc1 := bufio.NewScanner(of1)
	i := 0
	for sc1.Scan() {
		in := string(inSlice[i])
		out := sc1.Text()
		if out != in {
			t.Fatalf("Expected %v, but got %v", in, out)
		} else {
			t.Logf("Expected %v", in)
		}
		i++
	}

	of2, err := os.Open(file2.Name() + ".out")
	if err != nil {
		t.Fatal(err)
	}
	defer of2.Close()
	sc2 := bufio.NewScanner(of2)
	i = 0
	for sc2.Scan() {
		in := string(inSlice[i])
		out := sc2.Text()
		if out != in {
			t.Fatalf("Expected %v, but got %v", in, out)
		} else {
			t.Logf("Expected %v", in)
		}
		i++
	}
}

func BenchmarkRankBytes(b *testing.B) {
	rand.Seed(9893489983248324)
	for i := 0; i < b.N; i++ {
		randBytes(20)
	}
}
