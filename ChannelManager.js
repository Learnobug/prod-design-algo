import {EventEmitter} from 'node:events';

export default class ChannelManager extends EventEmitter {

    constructor(opts = {}) {
        super();
        this.channels = null;
        this.logger = opts.logger ?? console;
        this._rabbitmq = opts.rabbitmq;

        this._channel = null;
        this._connection = null;
        this._connectingwaiters = [];

    }

    async getChannel() {
        if (this._channel) {
            return this._channel;
        }
        if (this._connectingwaiters.length > 0) {
            return new Promise((resolve, reject) => {
                this._connectingwaiters.push({ resolve, reject });
            });
        }
        return this._connect();
    }

    async _connect() {
        this._connection = true;
        try{
            let connection;

            if(this._rabbitmq.connection){
                connection = this._rabbitmq.connection;
            }else{
                baseChanel = await this._rabbitmq.connect();
                if(!baseChanel.connection){
                    throw new Error('Failed to connect to RabbitMQ');
                }
                connection = baseChanel.connection;

            }
            const confirmchannel = await connection.createConfirmChannel();

            confirmchannel.on('drain',()=>{
                this.emit('drain');
            });

            confirmchannel.on('error',(err)=>{
                this.logger.error(`[ChannelManager] channel error: ${err.message}`);
                this._handleConnectionError(err);
            });

            confirmchannel.on('close',()=>{
                this.logger.warn(`[ChannelManager] channel closed`);
                this._handleConnectionError(new Error('Channel closed'));
            });

            this._channel = confirmchannel;
            this.logger.info(`[ChannelManager] channel created successfully`);
            this._connectingwaiters.forEach(waiter => waiter.resolve(this._channel));
            this._connectingwaiters = [];
            return this._channel;

        }
        catch(err){
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

