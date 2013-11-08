/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rx.netty.experimental.protocol.http;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;

import java.util.List;

/**
 * An decoder for server-sent event. It does not record retry or last event ID. Otherwise, it
 * follows the same interpretation logic as documented here: <a href="http://www.whatwg.org/specs/web-apps/current-work/multipage/comms.html#event-stream-interpretation">Event Stream Interpretation</a>
 */
public class ServerSentEventDecoder extends ReplayingDecoder<ServerSentEventDecoder.State> {
    private final MessageBuffer eventBuffer;

    public ServerSentEventDecoder() {
        super(State.NEW_LINE);

        eventBuffer = new MessageBuffer();
    }

    private static boolean isLineDelimiter(char c) {
        return c == '\r' || c == '\n';
    }

    private static class MessageBuffer {
        public static final String DEFAULT_EVENT_NAME = "message";

        private final StringBuilder eventName = new StringBuilder();
        private final StringBuilder eventData = new StringBuilder();
        private final StringBuilder eventId = new StringBuilder();

        public MessageBuffer() {
            reset();
        }

        public MessageBuffer setEventName(String eventName) {
            this.eventName.setLength(0);
            this.eventName.append(eventName);

            return this;
        }

        public MessageBuffer setEventData(String eventData) {
            this.eventData.setLength(0);
            this.eventData.append(eventData);

            return this;
        }

        public MessageBuffer appendEventData(String eventData) {
            this.eventData.append(eventData);

            return this;
        }

        public MessageBuffer setEventId(String id) {
            this.eventId.setLength(0);
            this.eventId.append(id);

            return this;
        }

        public Message toMessage() {
            Message message = new Message(
                this.eventId.toString(),
                this.eventName.toString(),
                this.eventData.toString()
            );

            reset();

            return message;
        }

        public void reset() {
            this.eventId.setLength(0);

            this.eventName.setLength(0);
            this.eventName.append(DEFAULT_EVENT_NAME);

            this.eventData.setLength(0);
        }
    }


    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state()) {
            case NEW_LINE:
                String line = readFullLine(in);

                // Immediately checkpoint the progress so that replaying decoder
                // will not start over from the beginning of the line when buffer overflows
                checkpoint();

                int colonIndex = line.indexOf(':');

                if(colonIndex <= 0){
                    // Comment or line without field name, ignore it.
                    break;
                }

                String type = line.substring(0, colonIndex).trim();
                if(type.equals("data")){
                    eventBuffer.appendEventData(line.substring(colonIndex+1));
                }

                if(type.equals("event")){
                    eventBuffer.setEventName(line.substring(colonIndex+1).trim());
                }

                if(type.equals("id")) {
                    eventBuffer.setEventId(line.substring(colonIndex+1).trim());
                }

                break;
            case END_OF_LINE:
                int skipped = skipLineDelimiters(in);
                if(skipped > 0) {
                    out.add(eventBuffer.toMessage());
                    eventBuffer.reset();
                }
                break;
        }
    }

    private String readFullLine(ByteBuf in) {
        StringBuilder line = new StringBuilder();

        for (; ; ) {
            char c = (char) in.readByte();
            if (isLineDelimiter(c)) {
                // Storing new line makes it convenient to process data because we need to
                // conserve the new line for multi-line data anyway. Also, if colon is at
                // the very end of a line, having a new line will help us avoid IndexOutOfBoundException
                // without checking the colon is at the end of the line.
                line.append(c);

                checkpoint(State.END_OF_LINE);

                return line.toString();
            }

            line.append(c);
        }
    }

    private int skipLineDelimiters(ByteBuf in) {
        int skipped = 0;
        for (; ; ) {
            char c = (char) in.readByte();
            if (isLineDelimiter(c)) {
                skipped += 1;
                continue;
            }

            // Leave the reader index at the first letter of the next line, if any
            in.readerIndex(in.readerIndex() - 1);
            checkpoint(State.NEW_LINE);
            break;
        }

        return skipped;
    }

    public static enum State {
        NEW_LINE,
        END_OF_LINE
    }
}
