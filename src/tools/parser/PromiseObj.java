package tools.parser;

public class PromiseObj extends MsgObj {
    long promiseId;

    public PromiseObj(long messageId, long senderId, long parentMsgId, long promiseId) {
        super(messageId, senderId, parentMsgId);
        this.promiseId = promiseId;
    }
}
