import { Monitor, MonitorCheckReportList, MonitorCheckReport, IMonitorConfig } from '@agent.smith/monitor-base';
import { httpreq } from 'h2tp';

export interface ISimpleRESTMonitorConfig extends IMonitorConfig {
    apiURL: string;
}

export abstract class SimpleRESTMonitor extends Monitor {
    public apiURL: string;

    public constructor(config: ISimpleRESTMonitorConfig) {
        super(config);
        this.apiURL = config.apiURL;
    }

    protected abstract validate(body: string): Promise<MonitorCheckReportList>;

    public check(): Promise<MonitorCheckReportList> {
        return httpreq(this.apiURL).then(r => {
            if (r.response.statusCode === 200) {
                return this.validate(r.body);
            }
            const report: MonitorCheckReport = {
                status: 'down',
                message: `invalid status ${r.response.statusCode}`,
            };
            const list: MonitorCheckReportList = {
                version: '1.0',
                id: this.id,
                name: this.name,
                reports: [report],
            };
            return list;
        });
    }
}
