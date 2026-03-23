import ChannelManager from "./ChannelManager.js";
import { CircuitBreaker } from "./circuit_breaker.js";
import { RetryStrategy } from "./RetryStrategy.js";
import { EventProducer } from "./EventProducer.js";

export function createEventProducer(opts = {}) {

    const log = opts.logger ?? console;
    const rabbitmq = opts.rabbitmq;
    const queueName = opts.queuename ?? "events";

    //validate options

    if(!rabbitmq){
        throw new Error("RabbitMQ instance is required to create EventProducer");
    }
    if(!queueName){
        throw new Error("Queue name is required to create EventProducer");
    }


    const channelManager = opts.channelManager ?? new ChannelManager({ rabbitmq, logger: log });
    const circuitBreaker = opts.circuitBreaker ?? new CircuitBreaker({
        failureThreashold: opts.failureThreashold ?? 5,
        cooldownms: opts.cooldownms ?? 5000,
        halfOpenmaxAttempts: opts.halfOpenmaxAttempts ?? 3,
        logger: log
     });

     const retryStrategy = opts.retryStrategy ?? new RetryStrategy({
        maxRetries: opts.maxRetries ?? 3,
        retryDelay: opts.retryDelay ?? 1000,
     });
     return new EventProducer({
        channelManager,
        retryStrategy,
        circuitBreaker,
        logger: log,
        queuename: queueName,
     });



}
