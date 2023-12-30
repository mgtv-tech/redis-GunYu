package util

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetCallerStack(t *testing.T) {
	sf := GetCallerStack(1, 3)
	assert.Equal(t, 2, len(sf.Frames))
	assert.True(t, strings.HasSuffix(sf.Frames[0].FuncName, "TestGetCallerStack"))
	str := sf.String()
	assert.True(t, strings.Contains(str, "TestGetCallerStack"))

	str = sf.StringOneLine()
	fmt.Println(str)

}

func TestGetCallerStackBySize(t *testing.T) {
	str := string(GetStackStringBySize(0))
	assert.True(t, strings.Contains(str, "TestGetCallerStackBySize"))
}

func TestGetCallerFrame(t *testing.T) {
	fn := "TestGetCallerFrame"
	frame := GetCallerStackFrame(0)
	assert.True(t, strings.HasSuffix(frame.FuncName, fn))
	assert.True(t, strings.Contains(frame.String(), fn))
}
