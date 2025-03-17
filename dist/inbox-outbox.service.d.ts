import { OnModuleInit } from '@nestjs/common';
import { Connection } from 'mongoose';
import { ConfigService } from '@nestjs/config';
interface InboxOutboxConfig {
    database: string;
    collection: string;
    aggregation: (doc: any) => any[];
}
export declare class InboxOutboxService implements OnModuleInit {
    private readonly connection;
    private readonly configService;
    private readonly logger;
    private readonly maxRetries;
    private readonly inboxCollectionName;
    private readonly outboxCollectionName;
    private readonly deadLetterCollectionName;
    private readonly failedMessagesCollectionName;
    private readonly reshapedCollectionName;
    private aggregationPipeline;
    constructor(connection: Connection, configService: ConfigService);
    onModuleInit(): Promise<void>;
    initialize(config: InboxOutboxConfig): void;
    private getCollection;
    private watchInbox;
    addToOutbox(message: string): Promise<void>;
    private updateDocumentStatus;
    private processMessage;
    private runAggregation;
    private moveToFailedMessages;
    private deleteDocumentFromCollection;
    private retryMessage;
    retryFailedMessages(): Promise<void>;
    startRetryLoop(baseDelay?: number, maxBackoffTime?: number): Promise<void>;
}
export {};
