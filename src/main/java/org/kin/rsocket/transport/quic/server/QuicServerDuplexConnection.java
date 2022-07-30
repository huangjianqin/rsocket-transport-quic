package org.kin.rsocket.transport.quic.server;

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
import reactor.netty.NettyInbound;
import reactor.netty.NettyOutbound;
import reactor.netty.incubator.quic.QuicInbound;
import reactor.netty.incubator.quic.QuicOutbound;

import java.net.SocketAddress;

/**
 * quic server端duplex connection
 * @author huangjianqin
 * @date 2022/7/30
 */
public final class QuicServerDuplexConnection implements DuplexConnection {
    private static final Logger log = LoggerFactory.getLogger(QuicServerDuplexConnection.class);

    /** close signal */
    private final Sinks.Empty<Void> onClose = Sinks.empty();
    /** quic inbound */
    private final NettyInbound inbound;
    /** quic outbound */
    private NettyOutbound outbound;
    /** quic send frame flux */
    private final UnboundedProcessor sender = new UnboundedProcessor();

    /**
     * Creates a new instance
     */
    public QuicServerDuplexConnection(QuicInbound inbound, QuicOutbound outbound) {
        log.info("server connection created: " + inbound.getClass().getCanonicalName());
        this.outbound = outbound;
        this.inbound = inbound;
    }

    /**
     * connection准备好outbound
     */
    NettyOutbound prepareOutbound(){
        return outbound.send(sender);
    }

    @Override
    public ByteBufAllocator alloc() {
        return outbound.alloc();
    }

    @Override
    public SocketAddress remoteAddress() {
        // TODO: 2022/7/30
        return null;
    }

    @Override
    public void sendErrorAndClose(RSocketErrorException e) {
        final ByteBuf errorFrame = ErrorFrameCodec.encode(alloc(), 0, e);
        outbound
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
        log.info("begin to receive buffers from client!");
        return inbound.receive().map(FrameLengthCodec::frame);
    }

    @Override
    public void sendFrame(int streamId, ByteBuf frame) {
        sender.onNext(FrameLengthCodec.encode(alloc(), frame.readableBytes(), frame));
    }

    @Override
    public Mono<Void> onClose() {
        return onClose.asMono();
    }

    @Override
    public void dispose() {
        onClose.tryEmitEmpty();
    }
}
