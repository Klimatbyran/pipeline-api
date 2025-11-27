import { Job, JobType, Queue } from "bullmq";
import startQueues, { JOB_STATUS, QUEUE_NAMES, STATUS } from "../lib/bullmq";
import { BaseJob, CompanyProcess, DataJob, Process, QueueStatus } from "../schemas/types";

/**
 * Deep merge two objects, recursively merging nested objects
 */
function deepMerge(target: Record<string, any>, source: Record<string, any>): Record<string, any> {
    const output = { ...target };
    
    for (const key in source) {
        if (source.hasOwnProperty(key)) {
            const sourceValue = source[key];
            const targetValue = target[key];
            
            // If both values are objects (and not arrays or null), merge them recursively
            if (
                sourceValue !== null &&
                typeof sourceValue === 'object' &&
                !Array.isArray(sourceValue) &&
                targetValue !== null &&
                typeof targetValue === 'object' &&
                !Array.isArray(targetValue)
            ) {
                output[key] = deepMerge(targetValue, sourceValue);
            } else {
                // Otherwise, use the source value (or undefined to keep target)
                output[key] = sourceValue !== undefined ? sourceValue : targetValue;
            }
        }
    }
    
    return output;
}

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

    public async addJob(queueName: string, url: string, autoApprove: boolean = false, options?: { forceReindex?: boolean; threadId?: string; replaceAllEmissions?: boolean; runOnly?: string[] }): Promise<BaseJob> {
        const queue = await this.getQueue(queueName);
        const id = crypto.randomUUID();
        const job = await queue.add('download ' + url.slice(-20), {
            url: url.trim(),
            autoApprove,
            id,
            ...(options?.threadId ? { threadId: options.threadId } : {}),
            ...(options?.forceReindex !== undefined ? { forceReindex: options.forceReindex } : {}),
            ...(options?.replaceAllEmissions !== undefined ? { replaceAllEmissions: options.replaceAllEmissions } : {}),
            ...(options?.runOnly ? { runOnly: options.runOnly } : {})
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

    public async rerunJob(queueName: string, jobId: string, dataOverrides?: Record<string, any>): Promise<DataJob> {
        console.info('[QueueService] rerunJob: Starting', { queueName, jobId, hasDataOverrides: !!dataOverrides && Object.keys(dataOverrides).length > 0 });
        
        const queue = await this.getQueue(queueName);
        const job = await queue.getJob(jobId);
        
        if (!job) {
            console.error('[QueueService] rerunJob: Job not found', { queueName, jobId });
            throw new Error(`Job ${jobId} not found`);
        }
        
        const currentState = await job.getState();
        console.info('[QueueService] rerunJob: Job state', { queueName, jobId, currentState, jobName: job.name });
        
        // Update data if provided
        if (dataOverrides && Object.keys(dataOverrides).length > 0) {
            console.info('[QueueService] rerunJob: Updating job data', { queueName, jobId, dataOverrides });
            const currentData = job.data;
            // Use deep merge to preserve nested objects like approval
            const updatedData = deepMerge(currentData, dataOverrides);
            await job.updateData(updatedData);
            console.info('[QueueService] rerunJob: Job data updated', { queueName, jobId });
        }
        
        // Handle different states
        if (currentState === 'delayed') {
            console.info('[QueueService] rerunJob: Promoting delayed job', { queueName, jobId });
            await job.promote();
            const newState = await job.getState();
            console.info('[QueueService] rerunJob: Job promoted', { queueName, jobId, previousState: currentState, newState });
        } else if (currentState === 'failed') {
            console.info('[QueueService] rerunJob: Retrying failed job', { queueName, jobId });
            await job.retry();
            const newState = await job.getState();
            console.info('[QueueService] rerunJob: Job retried', { queueName, jobId, previousState: currentState, newState });
        } else if (currentState === 'completed') {
            console.info('[QueueService] rerunJob: Creating new job from completed job', { queueName, jobId, threadId: job.data?.threadId });
            // For completed jobs, create new one with same data (preserving threadId)
            const newJob = await queue.add(job.name, job.data);
            console.info('[QueueService] rerunJob: New job created', { queueName, originalJobId: jobId, newJobId: newJob.id });
            return this.getJobData(queueName, newJob.id!);
        } else if (['waiting', 'active'].includes(currentState)) {
            console.warn('[QueueService] rerunJob: Job already in runnable state', { queueName, jobId, currentState });
            throw new Error(`Job is already ${currentState}. Cannot re-run.`);
        } else {
            console.info('[QueueService] rerunJob: Moving job to waiting', { queueName, jobId, currentState });
            await job.moveToWaiting();
            const newState = await job.getState();
            console.info('[QueueService] rerunJob: Job moved to waiting', { queueName, jobId, previousState: currentState, newState });
        }
        
        const finalJob = await this.getJobData(queueName, jobId);
        console.info('[QueueService] rerunJob: Completed', { queueName, jobId, finalState: finalJob.status });
        return finalJob;
    }

    /**
     * From a follow-up job (e.g. scope1+2 or scope3), find the original
     * EXTRACT_EMISSIONS job for the same process/thread and enqueue a new
     * extract-emissions job with runOnly set to the requested scopes.
     */
    public async rerunExtractEmissionsFromFollowup(
        followupQueueName: string,
        followupJobId: string,
        scopes: string[],
    ): Promise<DataJob> {
        console.info('[QueueService] rerunExtractEmissionsFromFollowup: Starting', {
            followupQueueName,
            followupJobId,
            scopes,
        });

        const followupJob = await this.getFollowupJob(followupQueueName, followupJobId);
        const threadId = this.getThreadIdFromJob(followupJob);

        const extractEmissionsJob = await this.getLatestExtractEmissionsJobForThread(threadId);
        const fiscalYear = await this.getLatestFiscalYearForThread(threadId);

        const companyName = this.getCompanyNameFromJobs(
            extractEmissionsJob,
            followupJob,
            threadId
        );

        const mergedData = this.buildExtractRerunData(
            followupJob,
            extractEmissionsJob,
            fiscalYear,
            scopes
        );

        const newJob = await this.enqueueExtractRerun(companyName, mergedData);

        console.info('[QueueService] rerunExtractEmissionsFromFollowup: New job created', {
            newJobId: newJob.id,
            scopes,
        });

        return this.getJobData(QUEUE_NAMES.EXTRACT_EMISSIONS, newJob.id!);
    }

    private async getFollowupJob(
        followupQueueName: string,
        followupJobId: string
    ): Promise<DataJob> {
        return this.getJobData(followupQueueName, followupJobId);
    }

    private getThreadIdFromJob(job: DataJob): string {
        const followupData: any = job.data ?? {};

        const threadId =
            followupData.threadId ??
            job.threadId ??
            job.processId;

        if (!threadId) {
            console.error('[QueueService] getThreadIdFromJob: Missing threadId', {
                jobId: job.id,
            });
            throw new Error('Cannot locate process/thread for this job (no threadId).');
        }

        return threadId;
    }

    private async getLatestExtractEmissionsJobForThread(threadId: string): Promise<DataJob> {
        const extractJobs = await this.getDataJobs(
            [QUEUE_NAMES.EXTRACT_EMISSIONS],
            undefined,
            threadId
        );

        if (!extractJobs.length) {
            console.error('[QueueService] getLatestExtractEmissionsJobForThread: No EXTRACT_EMISSIONS job found', {
                threadId,
            });
            throw new Error('No EXTRACT_EMISSIONS job found for this process.');
        }

        return extractJobs.sort(
            (firstJob, secondJob) => (secondJob.timestamp ?? 0) - (firstJob.timestamp ?? 0)
        )[0];
    }

    private getCompanyNameFromJobs(
        extractEmissionsJob: DataJob,
        followupJob: DataJob,
        threadId: string
    ): string {
        const extractData: any = extractEmissionsJob.data ?? {};
        const followupData: any = followupJob.data ?? {};

        return (
            extractData.companyName ??
            followupData.companyName ??
            threadId
        );
    }

    private buildExtractRerunData(
        followupJob: DataJob,
        extractEmissionsJob: DataJob,
        fiscalYear: any | undefined,
        scopes: string[],
    ): any {
        const extractData: any = extractEmissionsJob.data ?? {};
        const followupData: any = followupJob.data ?? {};

        return {
            ...extractData,
            ...(followupData.wikidata ? { wikidata: followupData.wikidata } : {}),
            ...(fiscalYear ? { fiscalYear } : {}),
            runOnly: scopes,
        };
    }

    private async enqueueExtractRerun(
        companyName: string,
        jobData: any,
    ): Promise<Job> {
        const extractQueue = await this.getQueue(QUEUE_NAMES.EXTRACT_EMISSIONS);
        return extractQueue.add('rerun emissions ' + companyName, jobData);
    }

    private async getLatestFiscalYearForThread(threadId: string): Promise<any | undefined> {
        // For FOLLOW_UP_FISCAL_YEAR jobs, the fiscal year lives in the *return value* JSON, e.g.:
        // { "value": { "fiscalYear": { startMonth, endMonth } }, ... }.
        try {
            const fiscalJobs = await this.getDataJobs(
                [QUEUE_NAMES.FOLLOW_UP_FISCAL_YEAR],
                undefined,
                threadId
            );

            if (fiscalJobs.length === 0) {
                return undefined;
            }

            const latestFiscal = fiscalJobs.sort(
                (firstJob, secondJob) => (secondJob.timestamp ?? 0) - (firstJob.timestamp ?? 0)
            )[0];

            const returnValue = latestFiscal.returnvalue;
            if (typeof returnValue === 'string') {
                try {
                    const parsed = JSON.parse(returnValue);
                    return parsed.fiscalYear ?? parsed.value?.fiscalYear ?? undefined;
                } catch (parseErr) {
                    console.warn('[QueueService] getLatestFiscalYearForThread: Failed to parse fiscalYear returnvalue', {
                        threadId,
                        error: parseErr,
                    });
                    return undefined;
                }
            }

            if (returnValue && typeof returnValue === 'object') {
                const parsed: any = returnValue;
                return parsed.fiscalYear ?? parsed.value?.fiscalYear ?? undefined;
            }

            return undefined;
        } catch (err) {
            console.warn('[QueueService] getLatestFiscalYearForThread: Failed to fetch FOLLOW_UP_FISCAL_YEAR jobs', {
                threadId,
                error: err,
            });
            return undefined;
        }
    }

    /**
     * Re-run all jobs that match a given worker name (e.g. a value in data.runOnly[])
     * across one or more queues.
     *
     * By default it will re-run jobs that are either completed or failed, since
     * waiting/active jobs are already in progress.
     */
    public async rerunJobsByWorkerName(
        workerName: string,
        options?: {
            queueNames?: string[];
            statuses?: JobType[];
        }
    ): Promise<{ totalMatched: number; perQueue: Record<string, number> }> {
        const queueNames = options?.queueNames && options.queueNames.length > 0
            ? options.queueNames
            : Object.values(QUEUE_NAMES);

        const statuses = options?.statuses && options.statuses.length > 0
            ? options.statuses
            : (['completed', 'failed'] as JobType[]);

        console.info('[QueueService] rerunJobsByWorkerName: Starting', {
            workerName,
            queueNames,
            statuses
        });

        const perQueue: Record<string, number> = {};
        let totalMatched = 0;

        for (const queueName of queueNames) {
            const queue = await this.getQueue(queueName);
            const jobs = await queue.getJobs(statuses);

            const matchingJobs = jobs.filter(job => {
                const runOnly = job.data?.runOnly as string[] | undefined;
                return Array.isArray(runOnly) && runOnly.includes(workerName);
            });

            console.info('[QueueService] rerunJobsByWorkerName: Queue scan result', {
                queueName,
                totalJobs: jobs.length,
                matchingJobs: matchingJobs.length
            });

            for (const job of matchingJobs) {
                try {
                    await this.rerunJob(queueName, job.id!);
                } catch (error) {
                    console.error('[QueueService] rerunJobsByWorkerName: Failed to rerun job', {
                        queueName,
                        jobId: job.id,
                        error
                    });
                }
            }

            perQueue[queueName] = matchingJobs.length;
            totalMatched += matchingJobs.length;
        }

        console.info('[QueueService] rerunJobsByWorkerName: Completed', {
            workerName,
            totalMatched,
            perQueue
        });

        return { totalMatched, perQueue };
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