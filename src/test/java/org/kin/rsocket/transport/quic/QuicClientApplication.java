package org.kin.rsocket.transport.quic;

import io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.netty.incubator.codec.quic.QuicSslContext;
import io.netty.incubator.codec.quic.QuicSslContextBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.netty.NettyOutbound;
import reactor.netty.incubator.quic.QuicClient;
import reactor.netty.incubator.quic.QuicConnection;

import java.net.InetSocketAddress;
import java.time.Duration;

/**
 * 官方demo
 * @author huangjianqin
 * @date 2022/7/30
 */
public class QuicClientApplication {
	public static void main(String[] args) throws Exception {
		QuicSslContext clientCtx =
				QuicSslContextBuilder.forClient()
				                     .trustManager(InsecureTrustManagerFactory.INSTANCE)
				                     .applicationProtocols("rsocket/1.0")
				                     .build();

		QuicConnection client =
				QuicClient.create()
				          .bindAddress(() -> new InetSocketAddress(0))
				          .remoteAddress(() -> new InetSocketAddress("127.0.0.1", 7878))
				          .secure(clientCtx)
				          .wiretap(false)
				          .idleTimeout(Duration.ofSeconds(50))
				          .initialSettings(spec ->
				              spec.maxData(10000000)
				                  .maxStreamDataBidirectionalLocal(1000000))
				          .connectNow();

		Sinks.Many<String> sink = Sinks.many().multicast().onBackpressureBuffer();
		client.createStream((in, out) -> {
					//打印接受到的消息
					in.receive()
							.asString()
							.doOnNext(s -> {
								System.out.println("CLIENT RECEIVED: " + s);
							})
							.then()
							.subscribe();
					//外部控制具体发送什么消息
					return out.sendString(sink.asFlux());
				})
		      .subscribe();

		Flux.interval(Duration.ofSeconds(2)).subscribe(time -> sink.tryEmitNext("Hello!"));

		Thread.currentThread().join();
	}
}

