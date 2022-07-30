package org.kin.rsocket.transport.quic.server;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import io.rsocket.Closeable;
import io.rsocket.transport.ServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.incubator.quic.QuicServer;

import java.time.Duration;
import java.util.Objects;

/**
 * @author huangjianqin
 * @date 2022/7/30
 */
public final class QuicServerTransport implements ServerTransport<Closeable> {
    private static final Logger log = LoggerFactory.getLogger(QuicServerTransport.class);

    /** 绑定的端口 */
    private final int port;

    public static QuicServerTransport create(int port) {
        return new QuicServerTransport(port);
    }

    private QuicServerTransport(int port) {
        this.port = port;
    }

    @Override
    public Mono<Closeable> start(ConnectionAcceptor acceptor) {
        Objects.requireNonNull(acceptor, "acceptor must not be null");
        //quic server
        final QuicServer quicServer = createServer();
        return quicServer
                //处理inbound和outbound
                .handleStream((in, out) -> {
                    //创建quic server duplex connection
                    QuicServerDuplexConnection duplexConnection = new QuicServerDuplexConnection(in, out);
                    //accept connection
                    acceptor.apply(duplexConnection).subscribe(v -> log.info("acceptor initialized"));
                    //返回outbound, 不要在其他地方调用outbound.subscribe(...), 会报错, 意思是状态异常, 在连接建立前写入bytes
                    return duplexConnection.prepareOutbound();
                })
                .bind()
                .map(CloseableChannel::new);
    }

    /**
     * 获取quic ssl
     */
    @SuppressWarnings("unchecked")
    private <T extends Throwable> QuicSslContext quicSslContext() throws T {
        try {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            return QuicSslContextBuilder.forServer(ssc.privateKey(), null, ssc.certificate())
                    .applicationProtocols("rsocket/1.0")
                    .build();
        } catch (Throwable t) {
            throw (T)t;
        }
    }

    /**
     * 创建quic server
     */
    private QuicServer createServer() {
        return QuicServer.create()
                //写入或校验quic token
                .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                .host("0.0.0.0")
                .port(port)
                //不log connection event和content
                .wiretap(false)
                //ssl
                .secure(quicSslContext())
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
