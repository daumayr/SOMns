package tools.parser;

public class MsgObj {
    long messageId;
    long senderId;
    long receiverId;
    long parentMsgId;

    public MsgObj(long messageId, long senderId, long parentMsgId) {
        this.messageId = messageId;
        this.senderId = senderId;
        this.parentMsgId = parentMsgId;
    }


    public MsgObj(long messageId,long senderId, long receiverId, long parentMsgId) {
        this.messageId = messageId;
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.parentMsgId = parentMsgId;
    }
}
