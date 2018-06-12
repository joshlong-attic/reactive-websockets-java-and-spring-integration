package com.example.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.transformer.FileToStringTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import static org.springframework.web.reactive.function.server.RequestPredicates.POST;
import static org.springframework.web.reactive.function.server.RouterFunctions.route;

@SpringBootApplication
public class IntegrationApplication {

		private final Log log = LogFactory.getLog(getClass());

		@Data
		public static class LogRequest {
				private String message;
		}

		@Bean
		SubscribableChannel logChannel() {
				return new PublishSubscribeChannel();
		}

		@Bean
		RouterFunction<ServerResponse> routes() {
				return route(POST("/request"), serverRequest -> {

						Flux<Boolean> sent = serverRequest
							.bodyToFlux(LogRequest.class)
							.map(LogRequest::getMessage)
							.map(str -> MessageBuilder
								.withPayload(str)
								.setHeader("X-Foo", "Bar")
								.build())
							.map(this.logChannel()::send);

						return ServerResponse.ok().body(sent, Boolean.class);
				});
		}


		///// websockets

		@Bean
		WebSocketHandlerAdapter webSocketHandlerAdapter() {
				return new WebSocketHandlerAdapter();
		}

		@Data
		@AllArgsConstructor
		@NoArgsConstructor
		public static class MessageEvent {
				private String message;
		}

		@Bean
		WebSocketHandler wsh() {

				class ForwardingMessageHandler implements MessageHandler {

						private final ObjectMapper objectMapper = new ObjectMapper();
						private final FluxSink<WebSocketMessage> sink;
						private final WebSocketSession wsSession;

						ForwardingMessageHandler(FluxSink<WebSocketMessage> sink, WebSocketSession wsh) {
								this.sink = sink;
								this.wsSession = wsh;
						}

						@Override
						public void handleMessage(Message<?> message) throws MessagingException {

								String strPayloadFromChannel = String.class.cast(message.getPayload());

								MessageEvent me = new MessageEvent(strPayloadFromChannel);

								try {
										String json = this.objectMapper.writeValueAsString(me);
										WebSocketMessage wsMsg = wsSession.textMessage(json);
										sink.next(wsMsg);
								}
								catch (JsonProcessingException e) {
										throw new RuntimeException(e);
								}
						}
				}

				return new WebSocketHandler() {

						private final SubscribableChannel channel = logChannel();

						private final Map<String, MessageHandler> connections = new ConcurrentHashMap<>();

						@Override
						public Mono<Void> handle(WebSocketSession session) {

								String sessionId = session.getId();

								Publisher<WebSocketMessage> delayedMessages = Flux
									.create((Consumer<FluxSink<WebSocketMessage>>) sink -> {
											connections.put(sessionId, new ForwardingMessageHandler(sink, session));
											channel.subscribe(connections.get(sessionId));
									})
									.onErrorResume(Exception.class, Flux::error)
									.doOnComplete(() -> log.info("goodbye!"))
									.doFinally(signalType -> {
											log.info("goodbye cruel world! destroying session for " + sessionId);
											channel.unsubscribe(connections.get(sessionId));
											connections.remove(sessionId);
									});
								return session.send(delayedMessages);
						}
				};
		}

		@Bean
		HandlerMapping hm() {
				SimpleUrlHandlerMapping handlerMapping = new SimpleUrlHandlerMapping();
				handlerMapping.setUrlMap(Collections.singletonMap("/ws/files", wsh()));
				handlerMapping.setOrder(10);
				return handlerMapping;
		}


		/////
		@Bean
		IntegrationFlow fileFlow(@Value("${input-file:file:///${user.home}/Desktop/input}") File in) {
				return IntegrationFlows
					.from(Files.inboundAdapter(in)
							.autoCreateDirectory(true)
							.useWatchService(true)
						, poller -> poller.poller(pm -> pm.fixedRate(1000))
					)
					.transform(new FileToStringTransformer())
					.channel(this.logChannel())
					.get();
		}

		public static void main(String[] args) {
				SpringApplication.run(IntegrationApplication.class, args);
		}
}
