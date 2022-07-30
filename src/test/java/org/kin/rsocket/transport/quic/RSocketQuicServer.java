package org.kin.rsocket.transport.quic;


import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketServer;
import org.kin.rsocket.transport.quic.server.QuicServerTransport;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;

/**
 * @author huangjianqin
 * @date 2022/7/30
 */
public class RSocketQuicServer {
    public static void main(String[] args) {
        Hooks.onErrorDropped(Throwable::printStackTrace);
        RSocketServer.create((setup, sendingSocket) -> Mono.just(new RSocket() {
                    @Override
                    public Mono<Payload> requestResponse(Payload payload) {
                        System.out.println("server payload received: " + payload.getDataUtf8());
                        return Mono.just(payload);
                    }
                }))
                .bind(QuicServerTransport.create(7878))
                //.bind(TcpServerTransport.create(7878))
                .block()
                .onClose()
                .block();
    }
}
