package org.kin.rsocket.transport.quic;

import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.incubator.codec.quic.InsecureQuicTokenHandler;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import reactor.netty.Connection;
import reactor.netty.incubator.quic.QuicServer;

import java.time.Duration;

/**
 * 官方demo
 * @author huangjianqin
 * @date 2022/7/30
 */
public class QuicServerApplication {

    public static void main(String[] args) throws Exception {
        SelfSignedCertificate ssc = new SelfSignedCertificate();
        QuicSslContext serverCtx =
                QuicSslContextBuilder.forServer(ssc.privateKey(), null, ssc.certificate())
                        .applicationProtocols("rsocket/1.0")
                        .build();

        Connection server =
                QuicServer.create()
                        .host("0.0.0.0")
                        .port(7878)
                        .secure(serverCtx)
                        .tokenHandler(InsecureQuicTokenHandler.INSTANCE)
                        .wiretap(false)
                        .idleTimeout(Duration.ofSeconds(50))
                        .initialSettings(spec ->
                                spec.maxData(10000000)
                                        .maxStreamDataBidirectionalRemote(1000000)
                                        .maxStreamsBidirectional(100))
                        //直接转发
                        .handleStream((in, out) -> out.send(in.receive().retain()))
                        .bindNow();

        server.onDispose()
                .block();
    }
}
