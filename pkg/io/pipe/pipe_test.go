// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package pipe

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

func openPipe(t *testing.T, fileName string) (pr Reader, pw Writer, pf *os.File) {
	buffSize := 8192
	fileSize := 1024 * 1024 * 32
	if fileName == "" {
		pr, pw = NewSize(buffSize)
	} else {
		f, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
		MustNoError(err)
		pr, pw = NewFilePipe(fileSize, f)
		pf = f
	}
	return
}

func testPipe1(t *testing.T, fileName string) {
	r, w, f := openPipe(t, fileName)
	defer f.Close()

	s := "Hello world!!"

	go func(data []byte) {
		n, err := w.Write(data)
		MustNoError(err)
		Must(n == len(data))
		MustNoError(w.Close())
	}([]byte(s))

	buf := make([]byte, 64)
	n, err := io.ReadFull(r, buf)
	Must(errors.Is(err, io.EOF))
	Must(n == len(s))
	Must(string(buf[:n]) == s)
	MustNoError(r.Close())
}

func TestPipe1(t *testing.T) {
	testPipe1(t, "")
	testPipe1(t, "/tmp/pipe.test")
}

func testPipe2(t *testing.T, fileName string) {
	r, w, f := openPipe(t, fileName)
	defer f.Close()

	c := 1024 * 128
	s := "Hello world!!"

	go func() {
		for i := 0; i < c; i++ {
			m := fmt.Sprintf("[%d]%s ", i, s)
			n, err := w.Write([]byte(m))
			MustNoError(err)
			Must(n == len(m))
		}
		MustNoError(w.Close())
	}()

	time.Sleep(time.Millisecond * 10)

	buf := make([]byte, len(s)*c*2)
	n, err := io.ReadFull(r, buf)
	Must(errors.Is(err, io.EOF))
	buf = buf[:n]
	for i := 0; i < c; i++ {
		m := fmt.Sprintf("[%d]%s ", i, s)
		Must(len(buf) >= len(m))
		Must(string(buf[:len(m)]) == m)
		buf = buf[len(m):]
	}
	Must(len(buf) == 0)
	MustNoError(r.Close())
}

func TestPipe2(t *testing.T) {
	testPipe2(t, "")
	testPipe2(t, "/tmp/pipe.test")
}

func testPipe3(t *testing.T, fileName string) {
	r, w, f := openPipe(t, fileName)
	defer f.Close()

	c := make(chan int)

	size := 4096

	go func() {
		buf := make([]byte, size)
		for {
			n, err := r.Read(buf)
			if errors.Is(err, io.EOF) {
				break
			}
			MustNoError(err)
			c <- n
		}
		MustNoError(r.Close())
		c <- 0
	}()

	go func() {
		buf := make([]byte, size)
		for i := 1; i < size; i++ {
			n, err := w.Write(buf[:i])
			MustNoError(err)
			Must(n == i)
		}
		MustNoError(w.Close())
	}()

	sum := 0
	for i := 1; i < size; i++ {
		sum += i
	}
	for {
		n := <-c
		if n == 0 {
			break
		}
		sum -= n
	}
	Must(sum == 0)
}

func TestPipe3(t *testing.T) {
	testPipe3(t, "")
	testPipe3(t, "/tmp/pipe.test")
}

func testPipe4(t *testing.T, fileName string) {
	r, w, f := openPipe(t, fileName)
	defer f.Close()

	key := []byte("spinlock aes-128")

	block := aes.BlockSize
	count := 1024 * 1024 * 128 / block

	go func() {
		buf := make([]byte, count*block)
		m, err := aes.NewCipher(key)
		MustNoError(err)
		for i := 0; i < len(buf); i++ {
			buf[i] = byte(i)
		}

		e := cipher.NewCBCEncrypter(m, make([]byte, block))
		e.CryptBlocks(buf, buf)

		n, err := w.Write(buf)
		MustNoError(err)
		MustNoError(w.Close())
		Must(n == len(buf))
	}()

	buf := make([]byte, count*block)
	m, err := aes.NewCipher(key)
	MustNoError(err)

	n, err := io.ReadFull(r, buf)
	MustNoError(err)
	Must(n == len(buf))

	e := cipher.NewCBCDecrypter(m, make([]byte, block))
	e.CryptBlocks(buf, buf)

	for i := 0; i < len(buf); i++ {
		Must(buf[i] == byte(i))
	}
	_, err = io.ReadFull(r, buf)
	Must(errors.Is(err, io.EOF))
	MustNoError(r.Close())
}

func TestPipe4(t *testing.T) {
	testPipe4(t, "")
	testPipe4(t, "/tmp/pipe.test")
}

type pipeTest struct {
	async   bool
	err     error
	witherr bool
}

func (p pipeTest) String() string {
	return fmt.Sprintf("async=%v err=%v witherr=%v", p.async, p.err, p.witherr)
}

var pipeTests = []pipeTest{
	{true, nil, false},
	{true, nil, true},
	{true, io.ErrShortWrite, true},
	{false, nil, false},
	{false, nil, true},
	{false, io.ErrShortWrite, true},
}

func delayClose(t *testing.T, closer Closer, c chan int, u pipeTest) {
	time.Sleep(time.Millisecond * 10)
	var err error
	if u.witherr {
		err = closer.CloseWithError(u.err)
	} else {
		err = closer.Close()
	}
	MustNoError(err)
	c <- 0
}

func TestPipeReadClose(t *testing.T) {
	for _, u := range pipeTests {
		r, w := New()
		c := make(chan int, 1)

		if u.async {
			go delayClose(t, w, c, u)
		} else {
			delayClose(t, w, c, u)
		}

		buf := make([]byte, 64)
		n, err := r.Read(buf)
		<-c

		expect := u.err
		if expect == nil {
			expect = io.EOF
		}
		Must(errors.Is(err, expect))
		Must(n == 0)
		MustNoError(r.Close())
	}
}

func TestPipeReadClose2(t *testing.T) {
	r, w := New()
	c := make(chan int, 1)

	go delayClose(t, r, c, pipeTest{})

	n, err := r.Read(make([]byte, 64))
	<-c

	Must(errors.Is(err, io.ErrClosedPipe))
	Must(n == 0)
	MustNoError(w.Close())
}

func TestPipeWriteClose(t *testing.T) {
	for _, u := range pipeTests {
		r, w := New()
		c := make(chan int, 1)

		if u.async {
			go delayClose(t, r, c, u)
		} else {
			delayClose(t, r, c, u)
		}
		<-c

		n, err := w.Write([]byte("hello, world"))
		expect := u.err
		if expect == nil {
			expect = io.ErrClosedPipe
		}
		Must(errors.Is(err, expect))
		Must(n == 0)
		MustNoError(w.Close())
	}
}

func TestWriteEmpty(t *testing.T) {
	r, w := New()

	go func() {
		n, err := w.Write([]byte{})
		MustNoError(err)
		Must(n == 0)
		MustNoError(w.Close())
	}()

	time.Sleep(time.Millisecond * 10)

	buf := make([]byte, 4096)
	n, err := io.ReadFull(r, buf)
	Must(errors.Is(err, io.EOF))
	Must(n == 0)
	MustNoError(r.Close())
}

func TestWriteNil(t *testing.T) {
	r, w := New()

	go func() {
		n, err := w.Write(nil)
		MustNoError(err)
		Must(n == 0)
		MustNoError(w.Close())
	}()

	time.Sleep(time.Millisecond * 10)

	buf := make([]byte, 4096)
	n, err := io.ReadFull(r, buf)
	Must(errors.Is(err, io.EOF))
	Must(n == 0)
	MustNoError(r.Close())
}

func TestWriteAfterWriterClose(t *testing.T) {
	r, w := New()

	s := "hello"

	errs := make(chan error)

	go func() {
		n, err := w.Write([]byte(s))
		MustNoError(err)
		Must(n == len(s))
		MustNoError(w.Close())
		_, err = w.Write([]byte("world"))
		errs <- err
	}()

	buf := make([]byte, 4096)
	n, err := io.ReadFull(r, buf)
	Must(errors.Is(err, io.EOF))
	Must(string(buf[:n]) == s)

	err = <-errs
	Must(errors.Is(err, io.ErrClosedPipe))
	MustNoError(r.Close())
}

func TestWriteRead(t *testing.T) {
	r, w := New()
	p := make(chan []byte, 1)

	go func() {
		var x []byte
		for {
			b := make([]byte, 23)
			n, err := r.Read(b)
			if n != 0 {
				x = append(x, b[:n]...)
			}
			if err != nil {
				p <- x
				return
			}
		}
	}()

	b := make([]byte, 1024*1024*128)
	for i := 0; i < len(b); i++ {
		b[i] = byte(i)
	}
	n, err := w.Write(b)
	MustNoError(err)
	Must(n == len(b))

	w.Close()

	x := <-p
	Must(len(x) == len(b))
	Must(bytes.Equal(b, x))

	n, err = r.Read(b)
	Must(err != nil && n == 0)
}
