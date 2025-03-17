import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { InjectConnection } from '@nestjs/mongoose';
import { Connection, Types } from 'mongoose';
import { ConfigService } from '@nestjs/config';
import * as path from 'path';

interface DatabaseServiceConfig {
  database: string;
  collection: string;
  aggregation: (doc: any) => any[];
}

@Injectable()
export class DatabaseService implements OnModuleInit {
  private readonly logger = new Logger(DatabaseService.name);
  private readonly maxRetries: number = 3; // Default: 3 retries
  private readonly inboxCollectionName: string;
  private readonly outboxCollectionName: string;
  private readonly deadLetterCollectionName: string;
  private readonly failedMessagesCollectionName: string;
  private readonly reshapedCollectionName: string;
  private aggregationPipeline: (doc: any) => any[];

  constructor(
    @InjectConnection() private readonly connection: Connection,
    private readonly configService: ConfigService
  ) {
    const env = this.configService.get<string>('NODE_ENV');
    this.inboxCollectionName = env === 'production' ? 'inbox' : 'inbox_test';
    this.outboxCollectionName = env === 'production' ? 'outbox' : 'outbox_test';
    this.deadLetterCollectionName = env === 'production' ? 'dead_letter_inbox' : 'dead_letter_inbox_test';
    this.failedMessagesCollectionName = env === 'production' ? 'failed_messages' : 'failed_messages_test';
    this.reshapedCollectionName = env === 'production' ? 'reshaped_data' : 'reshaped_data_test';
  }

  async onModuleInit() {
    this.watchInbox();
    this.startRetryLoop(); // Start retry loop on init
  }

  initialize(config: DatabaseServiceConfig) {
    this.aggregationPipeline = config.aggregation;
  }

  private getCollection(collectionName: string) {
    return this.connection.collection(collectionName);
  }

  private watchInbox() {
    const inboxCollection = this.getCollection(this.inboxCollectionName);
    const changeStream = inboxCollection.watch([], { fullDocument: 'updateLookup' });

    changeStream.on('change', async (change) => {
      if ((change.operationType === 'update' || change.operationType === 'insert') && change.fullDocument) {
        await this.processMessage(change.fullDocument);
        this.logger.log(`New inbox entry detected: ${JSON.stringify(change.fullDocument)}`);
      } else {
        this.logger.log(`Change detected in INBOX: ${change}`);
        this.logger.log(`Change detected in INBOX: ${change.operationType}`);
      }
    });
    this.logger.log(`Watching inbox for new messages`);
  }

  async addToOutbox(message: string) {
    const outboxCollection = this.getCollection(this.outboxCollectionName);
    await outboxCollection.insertOne({ message, createdAt: new Date() });
  }

  private async updateDocumentStatus(collection: any, docs: any[], status: string) {
    const bulkOps = docs.map(doc => ({
      updateOne: {
        filter: { _id: doc._id },
        update: { $set: { status, processing_start: new Date().toISOString() } }
      }
    }));
    if (bulkOps.length) {
      await collection.bulkWrite(bulkOps);
    }
  }

  private async processMessage(messages: any | any[]) {
    const inboxCollection = this.getCollection(this.inboxCollectionName);
    const docs = Array.isArray(messages) ? messages : [messages];
    const toProcess = docs.filter(doc => doc?.status === 'enqueued');
    if (toProcess.length === 0) return;
    this.logger.log(`Processing ${toProcess.length} messages...`);

    await this.updateDocumentStatus(inboxCollection, toProcess, 'processing');

    const failedMessages: any[] = [];
    for (const doc of toProcess) {
      try {
        this.logger.log(`Processing message ${doc._id}: ${JSON.stringify(doc)}`);

        if (doc.trigger_error) {
          throw new Error(`Simulated processing failure`);
        }

        if (!this.aggregationPipeline) {
          throw new Error(`No aggregation pipeline provided`);
        }

        const reshapedData = await this.runAggregation(doc, this.aggregationPipeline);

        const reshapedCollection = this.getCollection(this.reshapedCollectionName);
        await reshapedCollection.insertOne(reshapedData);

        await this.updateDocumentStatus(inboxCollection, [doc], 'processed');
        this.logger.log(`Message ${doc._id} successfully processed.`);
      } catch (error) {
        this.logger.error(`Processing failed for ${doc._id}: ${error.message}`);
        failedMessages.push({ ...doc, retries: doc.retries !== undefined ? doc.retries : 0 });
      }
    }

    if (failedMessages.length > 0) {
      const deadLetterCollection = this.getCollection(this.deadLetterCollectionName);
      await deadLetterCollection.insertMany(
        failedMessages.map(doc => ({
          ...doc,
          status: 'failed',
          failed_at: new Date().toISOString()
        }))
      );
      await this.deleteDocumentFromCollection(inboxCollection, failedMessages);
      this.logger.warn(`${failedMessages.length} messages moved to dead_letter_inbox due to failure`);
    }
  }

  private async runAggregation(doc: any, pipeline: (doc: any) => any[]) {
    const inboxCollection = this.getCollection(this.inboxCollectionName);
    const result = await inboxCollection.aggregate(pipeline(doc)).toArray();
    return result[0];
  }

  private async moveToFailedMessages(failedCollection: any, deadLetterCollection: any, messages: any[]) {
    this.logger.log(`Moving ${messages.length} messages to failed_messages`);
    await failedCollection.insertMany(
      messages.map(msg => ({
        _id: msg._id,
        ...msg,
        status: 'permanently_failed',
        reason: 'Max retries reached',
        lastAttempt: new Date(),
        retries: (msg.retries || 0) + 1
      }))
    );
    await this.deleteDocumentFromCollection(deadLetterCollection, messages);
  }

  private async deleteDocumentFromCollection(collection: any, messages: any[]) {
    const ids = messages.map(msg => new Types.ObjectId(msg._id));
    await collection.deleteMany({ _id: { $in: ids } });
    this.logger.log(`Deleted ${messages.length} messages from collection.`);
  }

  private async retryMessage(inboxCollection: any, deadLetterCollection: any, messages: any[]) {
    this.logger.log(`Retrying ${messages.length} messages...`);
    await inboxCollection.insertMany(
      messages.map(msg => ({
        ...msg,
        status: 'enqueued',
        timestamp: new Date(),
        retries: (msg.retries || 0) + 1,
      }))
    );
    await this.deleteDocumentFromCollection(deadLetterCollection, messages);
  }

  async retryFailedMessages() {
    const inboxCollection = this.getCollection(this.inboxCollectionName);
    const deadLetterCollection = this.getCollection(this.deadLetterCollectionName);
    const failedCollection = this.getCollection(this.failedMessagesCollectionName);
    this.logger.log(`Checking for failed messages to retry...`);

    const failedMessages = await deadLetterCollection.find({ retries: { $lt: this.maxRetries } }).toArray();

    if (failedMessages.length === 0) {
      this.logger.log(`No messages to retry.`);
      return;
    }
    const toRetry = failedMessages.filter(msg => msg.retries + 1 < this.maxRetries);
    const toMoveToFailed = failedMessages.filter(msg => msg.retries + 1 >= this.maxRetries);

    if (toMoveToFailed.length > 0) {
      await this.moveToFailedMessages(failedCollection, deadLetterCollection, toMoveToFailed);
    }

    if (toRetry.length > 0) {
      await this.retryMessage(inboxCollection, deadLetterCollection, toRetry);
    }
  }

  async startRetryLoop(baseDelay = 5000, maxBackoffTime = 10000) {
    const retryFailedMessages = async (attempt = 0) => {
      await this.retryFailedMessages();

      const delay = Math.min(baseDelay * (2 ** attempt), maxBackoffTime);
      this.logger.log(`Next retry check in ${delay / 1000} seconds`);

      setTimeout(() => retryFailedMessages(attempt + 1), delay);
    };
    retryFailedMessages();
  }
}
