
//dont use these settings for production, it will set your broker on fire..
const batchOptions = {
  batchSize: 5,
  commitEveryNBatch: 1,
  concurrency: 1,
  commitSync: false,
  noBatchCommits: false
};

export const nativeConfig = {
  brokers: ['localhost:9092'],
  clientId: 'example-producer'
};
