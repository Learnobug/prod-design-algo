const EVENT_TYPE = {
  click: "click",
  input: "input",
  API_HIT: "api_hit",
  PAGE_VIEW: "page_view",
  ERROR: "error",
};

export class EventProducer {
  constructor({
    channelManager,
    retryStrategy,
    circuitBreaker,
    logger,
    queuename = "events",
  } = {}) {
    this.channelManager = channelManager;
    this.retryStrategy = retryStrategy;
    this.circuitBreaker = circuitBreaker;
    this.logger = logger ?? console;
    this._queuename = queuename;
    this.matrix = {
      published: 0,
      failed: 0,
      retriesExhausted: 0,
    };

    this.shuttingDown = false;
  }

  _incrementmatrix(type) {
    if (this.matrix[type] !== undefined) {
      this.matrix[type]++;
    }
  }

  async publish(eventdata, { correlationId, attempt = 0 } = {}) {
    const channel = await this.channelManager.getChannel();
    const message = {
      type: eventdata.type,
      data: eventdata.data,
      timestamp: new Date().toISOString(),
      attempt: attempt + 1,
    };

    const buffer = Buffer.from(JSON.stringify(message));
    const publishoptions = {
      persistent: true,
      contentType: "application/json",
      messageId: eventdata.eventId,
      correlationId,
      timestamp: Math.floor(Date.now() / 1000),
    };

    try {
      await new Promise((resolve, reject) => {
        const written = channel.publish(
          "",
          this._queuename,
          buffer,
          publishoptions,
          (err) => {
            if (err) {
              this.logger.error(`Failed to publish event: ${err.message}`);
              reject(err);
            } else {
              this._incrementmatrix("published");
              this.logger.info(
                `Event published: ${message.type} (correlationId=${correlationId}, attempt=${message.attempt})`,
              );
              resolve();
            }
          },
        );

        if (!written) {
          this.logger.warn("Channel buffer is full, waiting for drain event");
          const onDrainHandler = () => {
            this.logger.info("Channel buffer drained, resuming publish");
            resolve();
          };
          channel.once("drain", onDrainHandler);
        }
      });
      this.circuitBreaker.OnSuccess();
    } catch (err) {
      this._incrementmatrix("failed");
      this.circuitBreaker.OnFailure();

      if (this.retryStrategy && this.retryStrategy.shouldRetry(err, attempt)) {
        const delay = this.retryStrategy.getDelayWithJitter(attempt);
        this.logger.warn(
          `Retrying event publish (attempt=${attempt + 1}) after ${delay}ms`,
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
        return this.publish(eventdata, { correlationId, attempt: attempt + 1 });
      }

      if (attempt >= (this.retryStrategy?.maxRetries ?? 0)) {
        this._incrementmatrix("retriesExhausted");
      }
      throw err;
    }
  }

  async shutdown() {
    this.shuttingDown = true;
    await this.channelManager.close();
    this.logger.info(
      "Producer is shutting down. No new events will be accepted.",
    );
  }

  getstats() {
    return {
      circuitBreakerState: this.circuitBreaker.snapshot(),
      ...this.matrix,
    };
  }

  async publishHit(eventData, opts = {}) {
    if (this.shuttingDown) {
      this.logger.warn("Producer is shutting down. Rejecting new events.");
      throw new Error("Producer is shutting down");
    }
    if (!this.circuitBreaker.allowRequest()) {
      this.logger.warn("Circuit breaker is open. Rejecting event.");
      throw new Error("Circuit breaker is open");
    }
    const correlationId =
      opts.correlationId || `event-${Date.now()}-${Math.random()}`;
    const startMs = Date.now();
    let attempt = 0;
    while (true) {
      try {
        await this.publish(eventData, { correlationId, attempt });
        const latency = Date.now() - startMs;
        this.logger.info(`API hit event published successfully`, {
          correlationId,
          latency,
          attempt: attempt + 1,
          eventId: eventData.eventId,
        });
        return true;
      } catch (err) {
        this.logger.error(`Failed to publish API hit event: ${err.message}`, {
          correlationId,
          attempt: attempt + 1,
          eventId: eventData.eventId,
        });
        const canRetry =
          this.retryStrategy && this.retryStrategy.shouldRetry(err, attempt);
        if (!canRetry) {
          if (attempt >= (this.retryStrategy?.maxRetries ?? 0)) {
            this._incrementmatrix("retriesExhausted");
          }
          throw err;
        }

        await this.retryStrategy.wait(attempt);
        attempt++;
      }
    }
  }
}
