package org.kin.rsocket.transport.quic.client;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.netty.incubator.codec.quic.QuicStreamType;
import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.incubator.quic.QuicClient;
import reactor.netty.incubator.quic.QuicConnection;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

import static reactor.netty.ConnectionObserver.State.CONNECTED;

/**
 * @author huangjianqin
 * @date 2022/7/30
 */
public final class QuicClientTransport implements ClientTransport {
    private static final Logger log = LoggerFactory.getLogger(QuicClientTransport.class);
    /** quic client */
    private final QuicClient client;

    public static QuicClientTransport create(String bindAddress, int port) {
        Objects.requireNonNull(bindAddress, "bindAddress must not be null");
        QuicClient quicClient = createClient(() -> new InetSocketAddress(bindAddress, port));
        return create(quicClient);
    }

    public static QuicClientTransport create(QuicClient quicClient) {
        return new QuicClientTransport(quicClient);
    }

    private QuicClientTransport(QuicClient client) {
        this.client = client;
    }

    @Override
    public Mono<DuplexConnection> connect() {
        final QuicConnection quicConnection = client.streamObserve((conn, state) -> {
                    if (state == CONNECTED) {
                        log.info("connected with " + conn.getClass().getCanonicalName());
                    }
                })
                .connectNow();
        final QuicDuplexConnection duplexConnection = new QuicDuplexConnection();
        quicConnection.createStream(QuicStreamType.BIDIRECTIONAL, (in, out) -> {
                    //quic stream创建成功后的回调
                    in.withConnection(connection -> {
                        //暴露底层connection, 同时执行自定义逻辑
                        duplexConnection.setConnection(connection);
                        duplexConnection.setInbound(in);
                    });
                    return duplexConnection.prepareOutbound(out);
                })
                //createStream成功, 会触发Mono的OnComplete, 不会触发OnNext, 所以不能包含sub operator
                .block();
        return Mono.just(duplexConnection);
    }

    /**
     * 创建quic client
     */
    private static QuicClient createClient(Supplier<SocketAddress> remoteAddress) {
        //ssl, trust X.509
        QuicSslContext clientCtx = QuicSslContextBuilder.forClient()
                .trustManager(InsecureTrustManagerFactory.INSTANCE)
                .applicationProtocols("rsocket/1.0")
                .build();
        return QuicClient.create()
                .remoteAddress(remoteAddress)
                .bindAddress(() -> new InetSocketAddress(0))
                //不log connection event和content
                .wiretap(false)
                //ssl
                .secure(clientCtx)
                //连接空闲超时(毫秒)
                .idleTimeout(Duration.ofSeconds(50))
                .initialSettings(spec -> {
                    //额外的设置
                    spec.maxData(10000000)
                            .maxStreamDataBidirectionalLocal(1000000)
                            .maxStreamDataBidirectionalRemote(1000000)
                            .maxStreamDataUnidirectional(1000000)
                            .maxStreamsBidirectional(100)
                            .maxStreamsUnidirectional(100);
                });
    }
}
