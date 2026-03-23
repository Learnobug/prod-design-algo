const RETRIABLE_ERRORS = [
    'ETIMEDOUT',
    'ECONNRESET',
    'EAI_AGAIN',
    'ENOTFOUND',
    'ESOCKETTIMEDOUT',
    'ECONNREFUSED',
];

export class RetryStrategy {
    constructor({ maxRetries = 3, retryDelay = 1000 } = {}) {
        this.maxRetries = maxRetries;
        this.retryDelay = retryDelay;
        this.jitter = 0.1; // 10% jitter
    }

    shouldRetry(error, attempt) {
        if (attempt >= this.maxRetries) {
            return false;
        }
        if (error && error.code && RETRIABLE_ERRORS.includes(error.code)) {
            return true;
        }
        return false;
    }

    getDelay(attempt) {
        return this.retryDelay * Math.pow(2, attempt);
    }

    getDelayWithJitter(attempt) {
        const baseDelay = this.getDelay(attempt);
        const jitterValue = baseDelay * this.jitter * (Math.random() - 0.5) * 2;
        return baseDelay + jitterValue;
    }

    async retry(fn, attempt = 0) {
        try {
            return await fn();
        } catch (error) {
            if (this.shouldRetry(error, attempt)) {
                const delay = this.getDelayWithJitter(attempt);
                await new Promise(resolve => setTimeout(resolve, delay));
                return this.retry(fn, attempt + 1);
            }
            throw error;
        }
    }

    async wait(attempt) {
        if (attempt >= this.maxRetries) {
            return false;
        }
        const delay = this.getDelayWithJitter(attempt);
        await new Promise(resolve => setTimeout(resolve, delay));
        return true;
    }

}
