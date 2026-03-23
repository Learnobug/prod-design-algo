import {EventEmitter} from 'node:events';

export default class ChannelManager extends EventEmitter {

    constructor(opts = {}) {
        super();
        this.logger = opts.logger ?? console;
        this._rabbitmq = opts.rabbitmq;

        this._channel = null;
        this._connection = null;
        this._connecting = false;
        this._connectingwaiters = [];

    }

    async getChannel() {
        if (this._channel) {
            return this._channel;
        }
        if (this._connecting) {
            return new Promise((resolve, reject) => {
                this._connectingwaiters.push({ resolve, reject });
            });
        }
        return this._connect();
    }

    async _connect() {
        this._connecting = true;
        try{
            let connection;

            if(this._rabbitmq.connection){
                connection = this._rabbitmq.connection;
            }else{
                const baseChannel = await this._rabbitmq.connect();
                if(!baseChannel.connection){
                    throw new Error('Failed to connect to RabbitMQ');
                }
                connection = baseChannel.connection;

            }
            const confirmChannel = await connection.createConfirmChannel();

            confirmChannel.on('drain',()=>{
                this.emit('drain');
            });

            confirmChannel.on('error',(err)=>{
                this.logger.error(`[ChannelManager] channel error: ${err.message}`);
                this._handleConnectionError(err);
            });

            confirmChannel.on('close',()=>{
                this.logger.warn(`[ChannelManager] channel closed`);
                this._handleConnectionError(new Error('Channel closed'));
            });

            this._channel = confirmChannel;
            this._connecting = false;
            this.logger.info(`[ChannelManager] channel created successfully`);
            this._connectingwaiters.forEach(waiter => waiter.resolve(this._channel));
            this._connectingwaiters = [];
            return this._channel;

        }
        catch(err){
            this._connecting = false;
            this.logger.error(`[ChannelManager] connection error: ${err.message}`);
            this._connectingwaiters.forEach(waiter => waiter.reject(err));
            this._connectingwaiters = [];
            this._handleConnectionError(err);
            throw err;
        }
    }

    _handleConnectionError(err){
        this._channel = null;
        this._connection = null;
        this.emit('error', err);
    }
}
