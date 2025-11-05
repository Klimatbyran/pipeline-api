import { Job, JobType, Queue } from "bullmq";
import startQueues, { JOB_STATUS, QUEUE_NAMES, STATUS } from "../lib/bullmq";
import { BaseJob, CompanyProcess, DataJob, Process, QueueStatus } from "../schemas/types";

export class QueueService {
    private static queueService: QueueService | undefined;
    private queues: Record<string, Queue> | undefined;

    private constructor() {}

    public async getQueues() {
        if (!this.queues) {
            const queues = await startQueues();
            this.queues = queues.reduce((acc, queue) => {
                acc[queue.name] = queue;
                return acc;
            }, {} as Record<string, Queue>);
        }
        return this.queues;
    }

    public static async getQueueService() {
        if (!QueueService.queueService) {
            QueueService.queueService = new QueueService();
        }
        return QueueService.queueService;
    }

    public async getQueue(name: string) {
        if(this.queues === undefined)
            this.queues = await this.getQueues();

        const queue = this.queues[name];
        if (!queue) {
            const available = Object.keys(this.queues).join(', ');
            throw new Error(`Unknown queue: ${name}. Available: ${available}`);
        }
        return queue;
    }

    public async getJobs(queueNames?: string[], status?: string, processId?: string): Promise<BaseJob[]> {
        if(!queueNames  || queueNames.length === 0) {
            queueNames = Object.values(QUEUE_NAMES);
        }
        const queryStatus = status ? [status] : JOB_STATUS;
        const jobs: BaseJob[] = [];
        for(const queueName of queueNames) {
             const queue = await this.getQueue(queueName);
            const rawJobs = await queue.getJobs(queryStatus as JobType[]);
            const filteredRawJobs = processId
                ? rawJobs.filter(job => {
                    const pid = job.data?.threadId;
                    return pid === processId;
                })
                : rawJobs;
            const transformedJobs = await Promise.all(
                filteredRawJobs.map(job => transformJobtoBaseJob(job))
            );
            jobs.push(...transformedJobs);
        }
        return jobs;
    }

    public async getDataJobs(queueNames?: string[], status?: string, processId?: string): Promise<DataJob[]> {
        if(!queueNames  || queueNames.length === 0) {
            queueNames = Object.values(QUEUE_NAMES);
        }
        const queryStatus = status ? [status] : JOB_STATUS;
        const jobs: BaseJob[] = [];
        for(const queueName of queueNames) {
             const queue = await this.getQueue(queueName);
            const rawJobs = await queue.getJobs(queryStatus as JobType[]);
            const filteredRawJobs = processId
                ? rawJobs.filter(job => {
                    const pid = job.data?.threadId;
                    return pid === processId;
                })
                : rawJobs;
            const transformedJobs = await Promise.all(
                filteredRawJobs.map(async job => {
                    const dataJob: DataJob = await transformJobtoBaseJob(job);                    
                    dataJob.data = job.data;        
                    dataJob.returnvalue = job.returnvalue;
                    return dataJob;
                })
            );
            jobs.push(...transformedJobs);
        }
        return jobs;
    }

    public async addJob(queueName: string, url: string, autoApprove: boolean = false, options?: { forceReindex?: boolean; threadId?: string }): Promise<BaseJob> {
        const queue = await this.getQueue(queueName);
        const id = crypto.randomUUID();
        const job = await queue.add('download ' + url.slice(-20), {
            url: url.trim(),
            autoApprove,
            id,
            ...(options?.threadId ? { threadId: options.threadId } : {}),
            ...(options?.forceReindex !== undefined ? { forceReindex: options.forceReindex } : {})
        });
        return transformJobtoBaseJob(job);
    }

    public async getJobData(queueName: string, jobId: string): Promise<DataJob> {
        const queue = await this.getQueue(queueName);
        const job = await queue.getJob(jobId);
        if(!job) throw new Error(`Job ${jobId} not found`);
        const baseJob: DataJob = await transformJobtoBaseJob(job);
        baseJob.data = job.data;        
        baseJob.returnvalue = job.returnvalue;
        return baseJob;
    }

    public async getQueueStats(queueName?: string): Promise<QueueStatus[]> {
        const queues = queueName ? [await this.getQueue(queueName)] : Object.values(await this.getQueues());
        const stats: QueueStatus[] = [];
        for(const queue of queues) {
            const queueStats: Record<string, number> = {};
            const rawStats = await queue.getJobCounts(...(JOB_STATUS as JobType[]));
            for(const [key, value] of Object.entries(rawStats)) {
                queueStats[key] = value;
            }
            stats.push({
                name: queue.name,
                status: queueStats
            })
        }
        return stats;
    }
}

export async function transformJobtoBaseJob(job: Job): Promise<BaseJob> {
    return {
        name: job.name,
        queue: job.queueName,
        id: job.id,
        url: job.data.url ?? undefined,
        autoApprove: job.data.autoApprove ?? false,
        processId: job.data.threadId ?? undefined,
        threadId: job.data.threadId ?? undefined,
        timestamp: job.timestamp,
        processedBy: job.processedBy,
        finishedOn: job.finishedOn,
        attemptsMade: job.attemptsMade,
        failedReason: job.failedReason,
        stacktrace: job.stacktrace ?? [],
        approval: job.data.approval ? job.data.approval : undefined,
        progress: typeof job.progress === 'number' ? job.progress : undefined,
        opts: job.opts,
        delay: job.delay,
        status: (await job.getState()) as JobType
    };
}