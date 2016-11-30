package fileproc

import (
	"bufio"
	"context"
	"io"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

type Mapper interface {
	Map(data []byte) []byte
}

type Reducer interface {
	Reduce(data []byte) []byte
}

type processor struct {
	Mapper
	Reducer
	NumMapper    int
	mapperEG     errgroup.Group
	reducerEG    errgroup.Group
	writerEG     errgroup.Group
	ctx          context.Context
	cancel       context.CancelFunc
	inputLineCnt int
	mapOutCnt    int32
	redOutCnt    int
}

type lineInfo struct {
	Index int
	Bytes []byte
}

func newProcessor(num int, m Mapper, r Reducer) *processor {
	p := &processor{
		NumMapper: num,
		Mapper:    m,
		Reducer:   r,
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	return p
}

func (p *processor) stat() (int, int, int) {
	return p.inputLineCnt, int(p.mapOutCnt), p.redOutCnt
}

func (p *processor) run(r io.Reader, fw FileWriter) error {
	mapperChan := make(chan lineInfo, p.NumMapper)
	reducerChan := make(chan lineInfo, p.NumMapper)
	writerChan := make(chan lineInfo, p.NumMapper)
	if p.Reducer != nil {
		p.registerMapper(mapperChan, reducerChan)
		p.registerReducer(reducerChan, writerChan)
	} else {
		p.registerMapper(mapperChan, writerChan)
	}
	p.registerWriter(writerChan, fw)

	lineIndex := 0
	sc := bufio.NewScanner(r)
	sc.Buffer([]byte{}, 2*1024*1024) // default 64k, change to 2M
	for sc.Scan() {
		select {
		case <-p.ctx.Done():
			return p.ctx.Err()
		default:
			li := lineInfo{
				Index: lineIndex,
				Bytes: make([]byte, len(sc.Bytes())),
			}
			copy(li.Bytes, sc.Bytes())
			mapperChan <- li
			lineIndex++
		}
	}
	p.inputLineCnt += lineIndex

	close(mapperChan)
	p.mapperEG.Wait()

	if p.Reducer != nil {
		close(reducerChan)
		p.reducerEG.Wait()
	}

	close(writerChan)
	p.writerEG.Wait()

	return sc.Err()
}

func (p *processor) registerMapper(rc <-chan lineInfo, wc chan<- lineInfo) {
	for i := 0; i < p.NumMapper; i++ {
		p.mapperEG.Go(func() error {
			for li := range rc {
				// ignore error here, just for keep input sequence
				li.Bytes = p.Map(li.Bytes)
				if li.Bytes != nil {
					atomic.AddInt32(&p.mapOutCnt, 1)
				}
				wc <- li
			}
			return nil
		})
	}
}

func (p *processor) registerReducer(rc <-chan lineInfo, wc chan<- lineInfo) {
	p.reducerEG.Go(func() error {
		for li := range rc {
			// ignore error here, just for keep input sequence
			li.Bytes = p.Reduce(li.Bytes)
			if li.Bytes != nil {
				p.redOutCnt++
			}
			wc <- li
		}
		return nil
	})
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
