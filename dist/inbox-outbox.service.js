"use strict";
var __decorate = (this && this.__decorate) || function (decorators, target, key, desc) {
    var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
    if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
    else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
    return c > 3 && r && Object.defineProperty(target, key, r), r;
};
var __metadata = (this && this.__metadata) || function (k, v) {
    if (typeof Reflect === "object" && typeof Reflect.metadata === "function") return Reflect.metadata(k, v);
};
var __param = (this && this.__param) || function (paramIndex, decorator) {
    return function (target, key) { decorator(target, key, paramIndex); }
};
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var DatabaseService_1;
Object.defineProperty(exports, "__esModule", { value: true });
exports.DatabaseService = void 0;
const common_1 = require("@nestjs/common");
const mongoose_1 = require("@nestjs/mongoose");
const mongoose_2 = require("mongoose");
const config_1 = require("@nestjs/config");
let DatabaseService = DatabaseService_1 = class DatabaseService {
    constructor(connection, configService) {
        this.connection = connection;
        this.configService = configService;
        this.logger = new common_1.Logger(DatabaseService_1.name);
        this.maxRetries = 3; // Default: 3 retries
        this.aggregationPipeline = () => [];
        const env = this.configService.get('NODE_ENV');
        this.inboxCollectionName = env === 'production' ? 'inbox' : 'inbox_test';
        this.outboxCollectionName = env === 'production' ? 'outbox' : 'outbox_test';
        this.deadLetterCollectionName = env === 'production' ? 'dead_letter_inbox' : 'dead_letter_inbox_test';
        this.failedMessagesCollectionName = env === 'production' ? 'failed_messages' : 'failed_messages_test';
        this.reshapedCollectionName = env === 'production' ? 'reshaped_data' : 'reshaped_data_test';
    }
    onModuleInit() {
        return __awaiter(this, void 0, void 0, function* () {
            this.watchInbox();
            this.startRetryLoop(); // Start retry loop on init
        });
    }
    initialize(config) {
        this.aggregationPipeline = config.aggregation;
    }
    getCollection(collectionName) {
        return this.connection.collection(collectionName);
    }
    watchInbox() {
        const inboxCollection = this.getCollection(this.inboxCollectionName);
        const changeStream = inboxCollection.watch([], { fullDocument: 'updateLookup' });
        changeStream.on('change', (change) => __awaiter(this, void 0, void 0, function* () {
            if ((change.operationType === 'update' || change.operationType === 'insert') && change.fullDocument) {
                yield this.processMessage(change.fullDocument);
                this.logger.log(`New inbox entry detected: ${JSON.stringify(change.fullDocument)}`);
            }
            else {
                this.logger.log(`Change detected in INBOX: ${change}`);
                this.logger.log(`Change detected in INBOX: ${change.operationType}`);
            }
        }));
        this.logger.log(`Watching inbox for new messages`);
    }
    addToOutbox(message) {
        return __awaiter(this, void 0, void 0, function* () {
            const outboxCollection = this.getCollection(this.outboxCollectionName);
            yield outboxCollection.insertOne({ message, createdAt: new Date() });
        });
    }
    updateDocumentStatus(collection, docs, status) {
        return __awaiter(this, void 0, void 0, function* () {
            const bulkOps = docs.map(doc => ({
                updateOne: {
                    filter: { _id: doc._id },
                    update: { $set: { status, processing_start: new Date().toISOString() } }
                }
            }));
            if (bulkOps.length) {
                yield collection.bulkWrite(bulkOps);
            }
        });
    }
    processMessage(messages) {
        return __awaiter(this, void 0, void 0, function* () {
            const inboxCollection = this.getCollection(this.inboxCollectionName);
            const docs = Array.isArray(messages) ? messages : [messages];
            const toProcess = docs.filter(doc => (doc === null || doc === void 0 ? void 0 : doc.status) === 'enqueued');
            if (toProcess.length === 0)
                return;
            this.logger.log(`Processing ${toProcess.length} messages...`);
            yield this.updateDocumentStatus(inboxCollection, toProcess, 'processing');
            const failedMessages = [];
            for (const doc of toProcess) {
                try {
                    this.logger.log(`Processing message ${doc._id}: ${JSON.stringify(doc)}`);
                    if (doc.trigger_error) {
                        throw new Error(`Simulated processing failure`);
                    }
                    if (!this.aggregationPipeline) {
                        throw new Error(`No aggregation pipeline provided`);
                    }
                    const reshapedData = yield this.runAggregation(doc, this.aggregationPipeline);
                    const reshapedCollection = this.getCollection(this.reshapedCollectionName);
                    yield reshapedCollection.insertOne(reshapedData);
                    yield this.updateDocumentStatus(inboxCollection, [doc], 'processed');
                    this.logger.log(`Message ${doc._id} successfully processed.`);
                }
                catch (error) {
                    this.logger.error(`Processing failed for ${doc._id}: ${error.message}`);
                    failedMessages.push(Object.assign(Object.assign({}, doc), { retries: doc.retries !== undefined ? doc.retries : 0 }));
                }
            }
            if (failedMessages.length > 0) {
                const deadLetterCollection = this.getCollection(this.deadLetterCollectionName);
                yield deadLetterCollection.insertMany(failedMessages.map(doc => (Object.assign(Object.assign({}, doc), { status: 'failed', failed_at: new Date().toISOString() }))));
                yield this.deleteDocumentFromCollection(inboxCollection, failedMessages);
                this.logger.warn(`${failedMessages.length} messages moved to dead_letter_inbox due to failure`);
            }
        });
    }
    runAggregation(doc, pipeline) {
        return __awaiter(this, void 0, void 0, function* () {
            const inboxCollection = this.getCollection(this.inboxCollectionName);
            const result = yield inboxCollection.aggregate(pipeline(doc)).toArray();
            return result[0];
        });
    }
    moveToFailedMessages(failedCollection, deadLetterCollection, messages) {
        return __awaiter(this, void 0, void 0, function* () {
            this.logger.log(`Moving ${messages.length} messages to failed_messages`);
            yield failedCollection.insertMany(messages.map(msg => (Object.assign(Object.assign({ _id: msg._id }, msg), { status: 'permanently_failed', reason: 'Max retries reached', lastAttempt: new Date(), retries: (msg.retries || 0) + 1 }))));
            yield this.deleteDocumentFromCollection(deadLetterCollection, messages);
        });
    }
    deleteDocumentFromCollection(collection, messages) {
        return __awaiter(this, void 0, void 0, function* () {
            const ids = messages.map(msg => new mongoose_2.Types.ObjectId(msg._id));
            yield collection.deleteMany({ _id: { $in: ids } });
            this.logger.log(`Deleted ${messages.length} messages from collection.`);
        });
    }
    retryMessage(inboxCollection, deadLetterCollection, messages) {
        return __awaiter(this, void 0, void 0, function* () {
            this.logger.log(`Retrying ${messages.length} messages...`);
            yield inboxCollection.insertMany(messages.map(msg => (Object.assign(Object.assign({}, msg), { status: 'enqueued', timestamp: new Date(), retries: (msg.retries || 0) + 1 }))));
            yield this.deleteDocumentFromCollection(deadLetterCollection, messages);
        });
    }
    retryFailedMessages() {
        return __awaiter(this, void 0, void 0, function* () {
            const inboxCollection = this.getCollection(this.inboxCollectionName);
            const deadLetterCollection = this.getCollection(this.deadLetterCollectionName);
            const failedCollection = this.getCollection(this.failedMessagesCollectionName);
            this.logger.log(`Checking for failed messages to retry...`);
            const failedMessages = yield deadLetterCollection.find({ retries: { $lt: this.maxRetries } }).toArray();
            if (failedMessages.length === 0) {
                this.logger.log(`No messages to retry.`);
                return;
            }
            const toRetry = failedMessages.filter(msg => msg.retries + 1 < this.maxRetries);
            const toMoveToFailed = failedMessages.filter(msg => msg.retries + 1 >= this.maxRetries);
            if (toMoveToFailed.length > 0) {
                yield this.moveToFailedMessages(failedCollection, deadLetterCollection, toMoveToFailed);
            }
            if (toRetry.length > 0) {
                yield this.retryMessage(inboxCollection, deadLetterCollection, toRetry);
            }
        });
    }
    startRetryLoop(baseDelay = 5000, maxBackoffTime = 10000) {
        return __awaiter(this, void 0, void 0, function* () {
            const retryFailedMessages = (attempt = 0) => __awaiter(this, void 0, void 0, function* () {
                yield this.retryFailedMessages();
                const delay = Math.min(baseDelay * (Math.pow(2, attempt)), maxBackoffTime);
                this.logger.log(`Next retry check in ${delay / 1000} seconds`);
                setTimeout(() => retryFailedMessages(attempt + 1), delay);
            });
            retryFailedMessages();
        });
    }
};
DatabaseService = DatabaseService_1 = __decorate([
    (0, common_1.Injectable)(),
    __param(0, (0, mongoose_1.InjectConnection)()),
    __metadata("design:paramtypes", [mongoose_2.Connection,
        config_1.ConfigService])
], DatabaseService);
exports.DatabaseService = DatabaseService;
//# sourceMappingURL=inbox-outbox.service.js.map