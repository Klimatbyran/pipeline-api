import { QUEUE_NAMES } from "../lib/bullmq";
import { CompanyProcess, DataJob, Process, ProcessStatus } from "../schemas/types";
import { QueueService } from "./QueueService";

export class ProcessService {
    private static processService: ProcessService
    private queueService: QueueService;
    private constructor(queueService: QueueService) {
        this.queueService = queueService;
    }

    public static async getProcessService(): Promise<ProcessService> {
        if(!ProcessService.processService) {
            const queueService = await QueueService.getQueueService();
            ProcessService.processService = new ProcessService(queueService);
        }
        return ProcessService.processService;
    }

    public async getProcess(id: string): Promise<Process> {
        const jobs = await this.queueService.getDataJobs(undefined, undefined, id);
        return this.createProcess(jobs);
    }

    public async getProcesses(): Promise<Process[]> {
        const jobs = await this.queueService.getDataJobs(undefined, undefined);
        // Debug: log number of jobs fetched across all queues
        // Using console here; Fastify logger isn't directly available in service layer
        console.info('[ProcessService] getProcesses: jobs fetched', { count: jobs.length });
        const jobProcesses: Record<string, DataJob[]> = {};
        for(const job of jobs) {
            const key = job.data.threadId ?? "unknown";
            if(!jobProcesses[key]) {
                jobProcesses[key] = [];
            }
            jobProcesses[key].push(job);
        }
        const processes: Process[] = [];
        for(const jobProcess of Object.values(jobProcesses)) {
            processes.push(this.createProcess(jobProcess));
        }
        console.info('[ProcessService] getProcesses: processes built', { count: processes.length });
        return processes;
    }

    public async getProcessesGroupedByCompany(): Promise<CompanyProcess[]> {
            const processes = await this.getProcesses();
            const companyProcesses: Record<string, CompanyProcess> = {};
            for(const process of processes) {
                const company = process.company ?? "unknown";
                if(companyProcesses[company]) {
                    companyProcesses[company].processes.push(process);
                } else {
                    companyProcesses[company] = {
                        company: process.company,
                        processes: [process]
                    }
                }
                if(process.wikidataId && company !== "unknown") {
                    companyProcesses[company].wikidataId = process.wikidataId;    
                }
            }
            const grouped = Object.values(companyProcesses);
            console.info('[ProcessService] getProcessesGroupedByCompany: companies grouped', { count: grouped.length });
            return grouped;
        }

    private createProcess(jobs: DataJob[]): Process {
        let id: string | undefined;
        let wikidataId: string | undefined;	
        let company: string | undefined;
        let year: number | undefined;

        for(const job of jobs) {
            if(job.data.threadId) {
                id = job.data.threadId;
            }
            if(job.data.wikidata) {
                wikidataId = job.data.wikidata.node;
            }
            if(job.data.companyName) {
                company = job.data.companyName;
            }
            if(job.data.reportYear) {
                year = job.data.reportYear;
            }
        }

        const startedAt = Math.min(...jobs.map(job => job.timestamp));
        const finishedAt = jobs.reduce((completionTime, job) => {
            if(job.finishedOn === undefined || completionTime === undefined) {
                return undefined;
            } else {
                return Math.max(completionTime, job.finishedOn);
            }
        }, 0);
        
        jobs.map(job => {
            const {data, ...rest} = job;
            return {
                ...rest
            };
        })

        const process: Process = {
            id: id ?? "unknown",
            jobs,
            wikidataId,
            company,
            year,
            startedAt,
            finishedAt,
            status: this.getProcessStatus(jobs),
        }
        return process;
    }

    private getProcessStatus(jobs: DataJob[]): ProcessStatus {
        if(jobs.find(job => job.status === 'failed')) {
            return 'failed';
        }
        if(jobs.find(job => ['waiting', 'delayed', 'paused'].includes(job.status ?? ''))) {
            return 'waiting';
        }
        if(jobs.find(job => job.queue === QUEUE_NAMES.SEND_COMPANY_LINK && job.status === 'completed')) {
            return 'completed';
        }
        return 'active';
    }
}