package util

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
)

type FuncStackFrame struct {
	FuncName string
	File     string
	Line     int
}

func (r *FuncStackFrame) String() string {
	if r == nil {
		return "no frame"
	}
	return fmt.Sprintf("%s  %s:%d", r.FuncName, r.File, r.Line)
}

type FuncStack struct {
	Frames []*FuncStackFrame
}

func (s *FuncStack) StringOneLine() string {
	var b bytes.Buffer
	for i, r := range s.Frames {
		if i == 0 {
			fmt.Fprintf(&b, "[%s(%s:%d)]", r.FuncName, r.File, r.Line)
		} else {
			fmt.Fprintf(&b, "->[%s]", r.FuncName)
		}
	}
	return b.String()
}

func (s *FuncStack) String() string {
	var b bytes.Buffer
	for i, r := range s.Frames {
		fmt.Fprintf(&b, "(%d)  %s\n", len(s.Frames)-i-1, r.FuncName)
		fmt.Fprintf(&b, "        %s:%d\n", r.File, r.Line)
	}
	if len(s.Frames) != 0 {
		fmt.Fprint(&b, "......\n")
	}
	return b.String()
}

func GetCallerStack(skip, depth int) FuncStack {
	s := make([]*FuncStackFrame, 0, depth-skip)
	for i := 0; i < depth-skip; i++ {
		r := GetCallerStackFrame(skip + i)
		if r == nil {
			break
		}
		s = append(s, r)
	}
	return FuncStack{Frames: s}
}

func GetCallerStackFrame(skip int) *FuncStackFrame {
	pc, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return nil
	}
	fn := runtime.FuncForPC(pc)
	if fn == nil || strings.HasPrefix(fn.Name(), "runtime.") {
		return nil
	}
	return &FuncStackFrame{
		FuncName: fn.Name(),
		File:     file,
		Line:     line,
	}
}

func GetStackStringBySize(size int) []byte {
	if size == 0 {
		size = 10 * 1024
	}
	buf := make([]byte, size)
	runtime.Stack(buf, false)
	return buf
}
