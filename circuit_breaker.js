export const CircuitState = Object.freeze({
    OPEN : "OPEN",
    CLOSE : "CLOSE",
    HALF_OPEN : "HALF_OPEN"
})

// close
// on failure >= threshold  -> open -> cooldownstart
// after cooldown 
// state -> halfopen
//  halfopen - > fail - open
//               sccuess count -> close

export class CircuitBreaker{

    constructor(opts = {}){
        this.failureThreashold =  opts.failureThreashold ?? opts.failureThreshold ?? 5;
        this.cooldownms = opts.cooldownms ?? 5000;
        this.halfOpenmaxAttempts = opts.halfOpenmaxAttempts ?? 3;
        this.logger = opts.logger ?? console;

        this.failurecount  = 0;
        this._state =CircuitState.CLOSE;
        this._lastfailuretime = 0;
        this._halfopenAttempts  = 0;
        this._halfopensucesscount = 0;

    };

    _cooldownElapsed(){
        return Date.now() - this._lastfailuretime >= this.cooldownms;
    }


    _opencircuit(){
        this._state = CircuitState.OPEN;
        this._lastfailuretime = Date.now();
        this.logger.info(`[Circuit breaker] opened (failures=${this.failurecount}, cooldown=${this.cooldownms}ms)`);
    }
    _transitionto(state){
        const prevState = this._state;
        const newState = state;
        this._state = newState;

        if(newState == CircuitState.HALF_OPEN){
            this._halfopenAttempts = 0;
            this._halfopensucesscount = 0;
            this.logger.info(`[Circuit breaker] : ${prevState} => Half OPEN`)
        }
    }

    _reset(){
        this.failurecount = 0;
        this._halfopenAttempts = 0;
        this._halfopensucesscount = 0;

    }

    get state(){
        if(this._state === CircuitState.OPEN && this._cooldownElapsed()){
            this._transitionto(CircuitState.HALF_OPEN);
        }
        return this._state;
    }

    allowRequest(){
        if(this.state === CircuitState.OPEN){
            return false;
        }
        if(this.state === CircuitState.HALF_OPEN ){
            if(this._halfopenAttempts >= this.halfOpenmaxAttempts){
                    return false;
            }else{
                this._halfopenAttempts++;
            }
        }
        return true;
    }

    OnSuccess(){
        if(this._state  == CircuitState.HALF_OPEN){
            this._halfopensucesscount++;
            if(this._halfopensucesscount >= this.halfOpenmaxAttempts){
                this._transitionto(CircuitState.CLOSE);
                this._reset();
                this.logger.info('[Circuit breaker] : Half OPEN -> CLOSED')
                return;
            }
        }
        if(this.failurecount > 0){
            this.failurecount  = 0;
            this.logger.info('Failure count reseted on success');
        }

    }

    OnFailure(){
        if(this._state == CircuitState.HALF_OPEN){
            this.logger.info('[Circuit breaker] : Half OPEN -> OPEN')
            this._opencircuit();
            return;
        }
        this.failurecount++;
        this._lastfailuretime = Date.now();
        if(this.failurecount >= this.failureThreashold){
            this.logger.info('[Circuit breaker] : CLOSE -> OPEN')
            this._opencircuit();
        }
    }

    snapshot(){
        return {
            failurecount: this.failurecount,
            state: this._state,
            lastfailuretime: this._lastfailuretime,
            halfopenAttempts: this._halfopenAttempts,
            halfopensucesscount: this._halfopensucesscount
        }
    }



} 