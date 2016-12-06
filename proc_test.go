package fileproc

import (
	"bufio"
	"bytes"
	"strconv"
	"testing"
)

var lineCnt int = 4

func getSum() int {
	sum := 0
	for i := 0; i < lineCnt; i++ {
		sum += i
	}
	return sum
}

type LineMR struct {
	cache map[string]int
}

func (*LineMR) Map(line []byte) []byte {
	return line
}

func (w *LineMR) Reduce(line []byte) []byte {
	fields := bytes.Split(line, []byte{'\t'})
	i, _ := strconv.Atoi(string(fields[0]))
	key := string(fields[1])
	v, ok := w.cache[key]
	if !ok {
		w.cache[key] = i
		return nil
	}
	sum := v + i
	if sum != getSum() {
		w.cache[key] = sum
		return nil
	}
	delete(w.cache, key)

	var buf bytes.Buffer
	buf.Write(fields[1])
	buf.WriteRune('\t')
	buf.WriteString(strconv.Itoa(sum))
	buf.WriteRune('\n')
	return buf.Bytes()
}

func TestReducer(t *testing.T) {
	inNum := 1000
	inSlice := randByteSlice(inNum, 20)
	var buf bytes.Buffer
	for _, b := range inSlice {
		for j := 0; j < lineCnt; j++ {
			buf.WriteString(strconv.Itoa(j))
			buf.WriteRune('\t')
			buf.Write(b)
			buf.WriteRune('\n')
		}
	}

	lmr := &LineMR{cache: make(map[string]int)}
	bw := newBufferWriter()
	fp := newProcessor(10, 2, lmr, lmr)
	fp.run(bufio.NewReader(&buf), WithSequence(bw))

	outSlice := bytes.Split(bw.buf.Bytes(), []byte{'\n'})
	outSlice = outSlice[:len(outSlice)-1]
	outNum := len(outSlice)
	if outNum != inNum {
		t.Fatalf("Expected %v, but got %v", inNum, outNum)
	} else {
		t.Logf("Expected %v", inNum)
	}

	suffix := "\t" + strconv.Itoa(getSum())
	for i, out := range outSlice {
		inStr := string(inSlice[i]) + suffix
		outStr := string(out)
		if outStr != inStr {
			t.Fatalf("Expected %v, but got %v", inStr, outStr)
		} else {
			t.Logf("Expected %v", outStr)
		}
	}
}
