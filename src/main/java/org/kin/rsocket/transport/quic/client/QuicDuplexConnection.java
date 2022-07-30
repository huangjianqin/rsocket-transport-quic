package org.kin.rsocket.transport.quic.client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.DuplexConnection;
import io.rsocket.RSocketErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameLengthCodec;
import io.rsocket.internal.UnboundedProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.netty.Connection;
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.netty.incubator.quic.QuicOutbound;

import java.net.SocketAddress;

/**
 * @author huangjianqin
 * @date 2022/7/30
 */
public final class QuicDuplexConnection implements DuplexConnection {
    private static final Logger log = LoggerFactory.getLogger(QuicDuplexConnection.class);

    /** close signal */
    private final Sinks.Empty<Void> onClose = Sinks.empty();
    /** quic connection */
    private Connection connection;
    /** quic inbound */
    private NettyInbound inbound;
    /** quic send frame flux */
    private final UnboundedProcessor sender = new UnboundedProcessor();

    /**
     * connection准备好outbound
     */
    NettyOutbound prepareOutbound(QuicOutbound outbound){
        return outbound.send(sender);
    }

    @Override
    public ByteBufAllocator alloc() {
        return connection.channel().alloc();
    }

    @Override
    public SocketAddress remoteAddress() {
        return connection.channel().remoteAddress();
    }

    @Override
    public void sendErrorAndClose(RSocketErrorException e) {
        final ByteBuf errorFrame = ErrorFrameCodec.encode(alloc(), 0, e);
        connection.outbound()
                .sendObject(FrameLengthCodec.encode(alloc(), errorFrame.readableBytes(), errorFrame))
                .then()
                .subscribe(
                        null,
                        onClose::tryEmitError,
                        () -> {
                            final Throwable cause = e.getCause();
                            if (cause == null) {
                                onClose.tryEmitEmpty();
                            } else {
                                onClose.tryEmitError(cause);
                            }
                        });
    }

    @Override
    public Flux<ByteBuf> receive() {
        log.info("begin to receive buffers from server!");
        return inbound.receive().map(FrameLengthCodec::frame);
    }

    @Override
    public void sendFrame(int streamId, ByteBuf frame) {
        log.info("begin to send buffers to server!");
        sender.onNext(FrameLengthCodec.encode(alloc(), frame.readableBytes(), frame));
    }

    @Override
    public Mono<Void> onClose() {
        return onClose.asMono();
    }

    @Override
    public void dispose() {
        connection.dispose();
        onClose.tryEmitEmpty();
    }

    //setter
    void setConnection(Connection connection) {
        this.connection = connection;
    }

    void setInbound(NettyInbound inbound) {
        this.inbound = inbound;
    }
}
