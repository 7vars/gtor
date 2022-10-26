package gtor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSendOnlyMessage(t *testing.T) {
	msg := SendOnly(10)
	assert.NotNil(t, msg)
	assert.Equal(t, 10, msg.Data())
	assert.True(t, msg.Replied())
}

func TestReplyMessage(t *testing.T) {
	reply := make(chan interface{}, 1)
	defer close(reply)
	msg := Message(10, reply)
	assert.NotNil(t, msg)
	assert.Equal(t, 10, msg.Data())
	assert.False(t, msg.Replied())
	msg.Reply(20)
	assert.True(t, msg.Replied())
	assert.Equal(t, 20, <-reply)
}
