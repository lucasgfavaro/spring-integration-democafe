package com.lgf;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.Aggregator;
import org.springframework.integration.annotation.CorrelationStrategy;
import org.springframework.integration.annotation.Gateway;
import org.springframework.integration.annotation.IntegrationComponentScan;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.dsl.AggregatorSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.RouterSpec;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.dsl.support.Consumer;
import org.springframework.integration.dsl.support.GenericHandler;
import org.springframework.integration.router.ExpressionEvaluatingRouter;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.stream.CharacterStreamWritingMessageHandler;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.stereotype.Component;

import com.google.common.util.concurrent.Uninterruptibles;
import com.lgf.domain.Delivery;
import com.lgf.domain.Drink;
import com.lgf.domain.DrinkType;
import com.lgf.domain.Order;
import com.lgf.domain.OrderItem;

@SpringBootApplication // 1
@IntegrationComponentScan // 2
public class Application {

    public static void main(String[] args) throws Exception {
	ConfigurableApplicationContext ctx = SpringApplication.run(Application.class, args); // 3

	Cafe cafe = ctx.getBean(Cafe.class); // 4
	for (int i = 1; i <= 15; i++) { // 5
	    Order order = new Order(i);
	    order.addItem(DrinkType.LATTE, 2, false);
	    order.addItem(DrinkType.MOCHA, 3, true);
	    cafe.placeOrder(order);
	}

	System.out.println("Hit 'Enter' to terminate"); // 6
	System.in.read();
	ctx.close();
    }

    @MessagingGateway // 7
    public interface Cafe {

	@Gateway(requestChannel = "orders.input") // 8
	void placeOrder(Order order); // 9

    }

    private final AtomicInteger hotDrinkCounter = new AtomicInteger();

    private final AtomicInteger coldDrinkCounter = new AtomicInteger(); // 10

    @Autowired
    private CafeAggregator cafeAggregator; // 11

    @Bean(name = PollerMetadata.DEFAULT_POLLER)
    public PollerMetadata poller() { // 12
	return Pollers.fixedDelay(1000).get();
    }

    @Bean
    @SuppressWarnings("unchecked")
    public IntegrationFlow orders() { // 13
	return IntegrationFlows.from("orders.input") // 14
		.split("payload.items", (Consumer) null) // 15
		.channel(MessageChannels.executor(Executors.newCachedThreadPool()))// 16
		.route("payload.iced", // 17
			new Consumer<RouterSpec<ExpressionEvaluatingRouter>>() { // 18

			    @Override
			    public void accept(RouterSpec<ExpressionEvaluatingRouter> spec) {
				spec.channelMapping("true", "iced").channelMapping("false", "hot"); // 19
			    }

			})
		.get(); // 20
    }

    @Bean
    public IntegrationFlow icedFlow() { // 21
	return IntegrationFlows.from(MessageChannels.queue("iced", 10)) // 22
		.handle(new GenericHandler<OrderItem>() { // 23

		    @Override
		    public Object handle(OrderItem payload, Map<String, Object> headers) {
			Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
			System.out.println(Thread.currentThread().getName() + " prepared cold drink #"
				+ coldDrinkCounter.incrementAndGet() + " for order #" + payload.getOrderNumber() + ": "
				+ payload);
			return payload; // 24
		    }

		}).channel("output") // 25
		.get();
    }

    @Bean
    public IntegrationFlow hotFlow() { // 26
	return IntegrationFlows.from(MessageChannels.queue("hot", 10)).handle(new GenericHandler<OrderItem>() {

	    @Override
	    public Object handle(OrderItem payload, Map<String, Object> headers) {
		Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS); // 27
		System.out.println(
			Thread.currentThread().getName() + " prepared hot drink #" + hotDrinkCounter.incrementAndGet()
				+ " for order #" + payload.getOrderNumber() + ": " + payload);
		return payload;
	    }

	}).channel("output").get();
    }

    @Bean
    public IntegrationFlow resultFlow() { // 28
	return IntegrationFlows.from("output") // 29
		.transform(new GenericTransformer<OrderItem, Drink>() { // 30

		    @Override
		    public Drink transform(OrderItem orderItem) {
			return new Drink(orderItem.getOrderNumber(), orderItem.getDrinkType(), orderItem.isIced(),
				orderItem.getShots()); // 31
		    }

		}).aggregate(new Consumer<AggregatorSpec>() { // 32

		    @Override
		    public void accept(AggregatorSpec aggregatorSpec) {
			aggregatorSpec.processor(cafeAggregator, null); // 33
		    }

		}, null).handle(CharacterStreamWritingMessageHandler.stdout()) // 34
		.get();
    }

    @Component
    public static class CafeAggregator { // 35

	@Aggregator // 36
	public Delivery output(List<Drink> drinks) {
	    return new Delivery(drinks);
	}

	@CorrelationStrategy // 37
	public Integer correlation(Drink drink) {
	    return drink.getOrderNumber();
	}

    }

}