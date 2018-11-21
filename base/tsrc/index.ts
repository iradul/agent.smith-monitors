import * as Kafka from 'node-rdkafka';

export type MonitorCheckStatus = 'healthy' | 'down' | 'failing' | 'broken';

export interface MonitorCheckReportList {
    version: '1.0';
    id: string;
    name: string;
    reports: MonitorCheckReport[];
    time?: number;
    kafka?: {
        partition?: number;
        key?: string;
    };
};

export interface MonitorCheckReport {
    status: MonitorCheckStatus;
    custom_status?: string;
    message: string;
    name?: string;
}

export interface RetryPolicy {
    factor: number;
    min: number;
    max: number;
}

export interface IKafkaConfig {
    topic: string;
    'kafka.config': any;
    'topic.config'?: any;
}

export interface IMonitorConfig {
    id: string;
    name: string;
    initialInterval?: number;
    interval: number;
    kafka: IKafkaConfig;
    retry?: RetryPolicy;
    autoStart?: boolean;
}

export abstract class Monitor {
    public id: string;
    public name: string;
    public interval: number;
    public retry?: RetryPolicy;
    public lastReportList: MonitorCheckReportList | null = null;
    public lastRun: number = 0;
    public nextRun: number = 0;
    public initialInterval: number = 0;

    private producer: Kafka.Producer;
    private kafka: IKafkaConfig;
    private disconnectPromise: Promise<void> | null = null;
    private connectPromise: Promise<void> | null = null;
    private _enabled = false;
    private tid: any;
    private attempt: number = 0;
    private autoStart: boolean;

    public get isConnected() { return this.producer.isConnected(); }

    public constructor(config: IMonitorConfig) {
        this.id = config.id;
        this.name = config.name;
        this.interval = config.interval;
        this.initialInterval = config.initialInterval !== undefined ? Math.min(this.initialInterval, this.interval) : this.interval;
        this.retry = config.retry ? Object.assign({}, config.retry) : {
            factor: 2,
            min: Math.min(this.interval, 5000),
            max: this.interval,
        };
        this.kafka = config.kafka;
        this.producer = new Kafka.Producer(config.kafka['kafka.config'], config.kafka['topic.config']);
        if (config.autoStart === undefined || config.autoStart) {
            this.autoStart = true;
            this.connect();
        } else {
            this.autoStart = false;
        }
    }

    public get enable() {
        return this._enabled;
    }

    public set enable(value: boolean) {
        value = !!value && this.isConnected;
        if (value != this.enable) {
            this._enabled = !!value;
            if (this._enabled) {
                this.tid = setTimeout(() => this.run(), this.initialInterval);
                this.nextRun = Date.now() + this.initialInterval;
            } else {
                clearTimeout(this.tid);
                this.nextRun = 0;
            }
        }
    }

    public abstract check(): Promise<MonitorCheckReportList>;

    public run(): Promise<MonitorCheckReportList> {
        return this.check()
            .then(list => {
                // validate reports
                for (let i = 0; i < list.reports.length; i++) {
                    const report = list.reports[i];
                    if (report.status === undefined || report.message === undefined) {
                        list.reports.splice(i, 1, {
                            status: 'broken',
                            message: JSON.stringify(report),
                        });
                    }
                }
                if (!list.time) {
                    list.time = Date.now();
                }
                return list;
            })
            .catch(err => {
                const errList: MonitorCheckReportList = {
                    version: '1.0',
                    id: this.id,
                    name: this.name,
                    time: Date.now(),
                    reports: [{
                        status: 'down',
                        message: err.toString(),
                    }],
                }
                return errList;
            })
            .then(list => {
                this.lastReportList = list;
                this.lastRun = Date.now();
                const msg = JSON.stringify(list);
                try {
                    this.producer.produce(
                        this.kafka.topic,
                        list.kafka ? list.kafka.partition : undefined,
                        Buffer.from(msg),
                        list.kafka ? list.kafka.key : undefined,
                        undefined,
                    );
                } catch (err) {
                    // todo
                    console.error('A problem occurred when sending message: ' + msg);
                    console.error(err);
                }
                if (this.enable) {
                    let interval = this.interval;
                    // todo: [0] can bug
                    if (list.reports[0].status !== 'down') {
                        this.attempt = 0;
                    } else {
                        if (this.retry) {
                            interval = Math.round(Math.min(this.retry.min * Math.pow(this.retry.factor, this.attempt), this.retry.max));
                        }
                        this.attempt++;
                    }
                    this.tid = setTimeout(() => this.run(), interval);
                    this.nextRun = Date.now() + interval;
                }
                return list;
            });
    }

    public connect() {
        if (this.connectPromise) {
            return this.connectPromise;
        }
        if (this.isConnected) {
            return Promise.resolve();
        }

        this.connectPromise = new Promise<void>((res, rej) => {
            this.producer.connect(undefined);
            this.producer
                .on('ready', () => {
                    this.producer.setPollInterval(100);
                    if (this.autoStart) {
                        this.enable = true;
                    }
                    res();
                })
                .on('event.error', (err: any) => {
                    console.error(`Error from producer:\n${err.message || err}`);
                });
        });
        this.connectPromise
            .then(() => this.connectPromise = null)
            .catch(() => this.connectPromise = null);
        return this.connectPromise;
    }

    public disconnect() {
        if (this.disconnectPromise) {
            return this.disconnectPromise;
        }
        if (!this.isConnected) {
            return Promise.resolve();
        }

        this.disconnectPromise = new Promise<void>((res, rej) => {
            this.producer.flush(600000, (errFlush: any) => {
                if (errFlush) {
                    rej(errFlush);
                } else {
                    this.producer.disconnect((errProducer) => {
                        this.enable = false;
                        if (errProducer) {
                            rej(errProducer);
                        } else {
                            res();
                        }
                    });
                }
            });
        });

        this.disconnectPromise
            .then(() => this.disconnectPromise = null)
            .catch(() => this.disconnectPromise = null);
        return this.disconnectPromise;
    }
}
