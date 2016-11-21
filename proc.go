package fileproc

import (
	"bufio"
	"context"
	"io"

	"golang.org/x/sync/errgroup"
)

type LineProcessor interface {
	Process(data []byte) []byte
}

type processor struct {
	LineProcessor
	NumProc  int
	ctx      context.Context
	cancel   context.CancelFunc
	procEG   errgroup.Group
	writerEG errgroup.Group
}

type lineInfo struct {
	Index    int
	Bytes    []byte
	procChan chan lineInfo
}

func newProcessor(num int, lp LineProcessor) *processor {
	p := &processor{
		LineProcessor: lp,
		NumProc:       num,
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	return p
}

func (p *processor) proc(r io.Reader, fw FileWriter) error {
	procChan := make(chan lineInfo, p.NumProc)
	writerChan := make(chan lineInfo, p.NumProc)
	p.registerProc(procChan, writerChan)
	p.registerWriter(writerChan, fw)

	lineCount := 0
	sc := bufio.NewScanner(r)
	sc.Buffer([]byte{}, 2*1024*1024) // default 64k, change to 2M
	for sc.Scan() {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
			li := lineInfo{
				Index: lineCount,
				Bytes: make([]byte, len(sc.Bytes())),
			}
			copy(li.Bytes, sc.Bytes())
			procChan <- li
			lineCount++
		}
	}

	close(procChan)
	p.procEG.Wait()

	close(writerChan)
	p.writerEG.Wait()

	return sc.Err()
}

func (p *processor) registerProc(pc <-chan lineInfo, wc chan<- lineInfo) {
	for i := 0; i < p.NumProc; i++ {
		p.procEG.Go(func() error {
			for li := range pc {
				// ignore error here, just for keep input sequence
				li.Bytes = p.Process(li.Bytes)
				wc <- li
			}
			return nil
		})
	}
}

func (p *processor) registerWriter(wc <-chan lineInfo, fw FileWriter) {
	p.writerEG.Go(func() error {
		if err := fw.Open(); err != nil {
			p.cancel()
			drainChan(wc)
			return err
		}

		for li := range wc {
			if _, err := fw.Write(li); err != nil {
				p.cancel()
				drainChan(wc)
				return err
			}
		}

		if err := fw.Close(); err != nil {
			p.cancel()
			return err
		}

		return nil
	})
}

func drainChan(c <-chan lineInfo) {
	go func() {
		for range c {
		}
	}()
}
