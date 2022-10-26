package gtor

type Msg interface {
	Data() interface{}
	Reply(interface{})
	Replied() bool
}

type sendOnlyMessage struct {
	data interface{}
}

func SendOnly(v interface{}) Msg {
	return &sendOnlyMessage{v}
}

func (msg *sendOnlyMessage) Data() interface{} {
	return msg.data
}

func (msg *sendOnlyMessage) Reply(interface{}) {}

func (msg *sendOnlyMessage) Replied() bool { return true }

type replyMessage struct {
	data    interface{}
	replied bool
	reply   chan<- interface{}
}

func Message(v interface{}, reply chan<- interface{}) Msg {
	return &replyMessage{
		data:  v,
		reply: reply,
	}
}

func (msg *replyMessage) Data() interface{} {
	return msg.data
}

func (msg *replyMessage) Reply(v interface{}) {
	msg.reply <- v
	msg.replied = true
}

func (msg *replyMessage) Replied() bool {
	return msg.replied
}
