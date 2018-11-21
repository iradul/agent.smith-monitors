const assert = require('assert');
const { Monitor } = require('../jsrc/index');
const mochaConfig = require('../package.json').mocha;

describe('Monitor', () => {
    const monitor = new Monitor({
        id: 'test-' + Math.random().toString(),
        name: 'test-monitor',
        initialInterval: 0,
        interval: 1000,
        autoStart: false,
        kafka: {
            'topic': mochaConfig.kafka.topic,
            'kafka.config': {
                'bootstrap.servers': mochaConfig.kafka.brokers,
                'compression.codec': 'snappy',
                'log_level': 4
            },
            'topic.config': {},
        }
    });
    describe('#connect', () => {
        it('should connect', () => {
            return monitor.connect();
        });
    });
    describe('#run', () => {
        const reportMsg = Math.random().toString();
        monitor.check = () => Promise.resolve({
            id: monitor.id,
            name: monitor.name,
            time: Date.now(),
            reports: [{
                status: 'healthy',
                message: reportMsg,
            }],
        });
        it('should publish report message to Kafka', (done) => {
            // start publishing bunch of messages
            const tid = setInterval(() => {
                monitor.producer.produce(mochaConfig.kafka.topic, undefined, Buffer.from("ping"));
            }, 1000);
            const Kafka = require('node-rdkafka');
            const consumer = new Kafka.KafkaConsumer({
                'bootstrap.servers': mochaConfig.kafka.brokers,
                "group.id": "agent-smith-monitor-test",
                "enable.auto.commit": false
            });
            consumer.connect();
            consumer
                .on('ready', () => {
                    consumer.subscribe([mochaConfig.kafka.topic]);
                    consumer.consume();
                    let published = false;
                    consumer.on('data', (m) => {
                        clearInterval(tid);
                        const val = m.value.toString();
                        if (val === 'ping') {
                            if (!published) {
                                // it's time to test monitor
                                monitor.run();
                            }
                            published = true;
                        } else {
                            const json = JSON.parse(val);
                            assert.ok(json.id === monitor.id && json.reports[0].message === reportMsg);
                            consumer.disconnect();
                            done();
                            return;
                        }
                    });
                })
                .on('event.error', (err) => {
                    console.error(`Error from producer:\n${err.message || err}`);
                });
        }).timeout(30000);
    });
    describe('#disconnect', () => {
        it('should disconnect', () => {
            return monitor.disconnect();
        });
    });
});
