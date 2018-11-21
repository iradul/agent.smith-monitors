import { MonitorCheckReportList, MonitorCheckStatus } from '@agent.smith/monitor-base';
import { SimpleRESTMonitor, ISimpleRESTMonitorConfig } from '@agent.smith/monitor-rest';
import { ChangeDetector } from '@agent.smith/utils';

interface IStatus {
    title: string;
    status: string;
    uptime: string;
    server_time: string;
    start_time: string;
    compaction_percentage: string;
    time_to_next_compaction: string;
    first_message_time: string;
    inbound: {
        total_messages: number;
        message_per_second: string;
        total_traffic: string;
        rate: string;
        bad_format_messages: number;
        biggest_message: string;
    };
    outbound: {
        total_messages: number;
        message_per_second: string;
        total_traffic: string;
        rate: string;
        dropped_messages: number;
        transform_errors: number;
    };
    memory_usage: {
        rss: string;
        heapTotal: string;
        heapUsed: string;
        external: string;
    };
    transformer: any;
}

export type TransformerFieldTupleList = Array<[string, string, boolean, number, Array<string>]>;

export interface IK2KNodeMonitorConfig extends ISimpleRESTMonitorConfig {
    outbound?: boolean;
    compaction?: boolean;
    transformer_fileds?: TransformerFieldTupleList;
}

export class K2KNodeMonitor extends SimpleRESTMonitor {
    public outbound = true;
    public compaction = false;
    public transformer_fileds: TransformerFieldTupleList;
    private cd: ChangeDetector;
    private cdMessage: any = {
        'inbound.total_messages': 'Service is not consuming anything new',
        'outbound.total_messages': 'Service is not publishing anything new',
        'compaction_percentage': 'Service is stuck while compacting',
    };

    public constructor(config: IK2KNodeMonitorConfig) {
        super(config);
        this.cd = new ChangeDetector(['inbound.total_messages']);
        this.outbound = config.outbound === undefined || config.outbound;
        if (this.outbound) this.cd.add('outbound.total_messages', true, 10000);
        this.compaction = !!config.compaction;
        if (this.compaction) this.cd.add('compaction_percentage', true, 0, "100%");
        this.transformer_fileds = config.transformer_fileds ? config.transformer_fileds : [];
        this.transformer_fileds.forEach(f => {
            const
                field = f[0],
                message = f[1],
                shouldChange = f[2],
                detectInterval = f[3],
                ignores = f[4] ? f[4] : [];
            this.cd.add(field, shouldChange, detectInterval, ...ignores);
            this.cdMessage[field] = message;
        });
    }

    public validate(body: string): Promise<MonitorCheckReportList> {
        const k2kStats: IStatus = JSON.parse(body);
        let message = '';
        if (!this.compaction && k2kStats.status !== 'running') {
            message += `Service is not running \`status = ${k2kStats.status}\`.`;
        } else if (this.cd.detect(k2kStats)) {
            this.cd.detectedFailures.forEach(f => {
                message += this.cdMessage[f.field] + ` \`${f.field} = ${this.cd.getStringValue(k2kStats, f.field)}\`.\n`;
            });
        }

        let status: MonitorCheckStatus;
        if (message) {
            status = 'failing';
        } else {
            status = 'healthy';
            message = '```\n' + JSON.stringify(k2kStats, null, 4) + '\n```';
        }
        const list: MonitorCheckReportList = {
            version: '1.0',
            id: this.id,
            name: this.name,
            reports: [{ status, message }],
        };
        return Promise.resolve(list);
    }
}
