const _ = require('lodash');

const addResultWithModel = (data, { Promise, modelFactory, config }) => {
  return new Promise(data, (res, rej) => {
    if (_.isNil(config.batchServiceFactory.dbName)) throw new Error('batchServiceFactory: Invalid parameters: config.batchServiceFactory.dbName is null or undefined');

    const Queue = modelFactory.get(config.batchServiceFactory.dbName, 'Queue');
    const QueueResult = modelFactory.get(config.batchServiceFactory.dbName, 'QueueResult');

    const results = {};
    Queue
      .findById(data.queueId)
      .then((queue) => {
        if (!queue) {
          throw new Error('Queue not found');
        }

        return QueueResult.create(data);
      })
      .then((queueResult) => {
        results.queueResult = queueResult;

        return Queue.update({
          id: data.queueId,
          status: data.queueStatus
        });
      })
      .then(() => {
        return res(results.queueResult);
      })
      .catch(rej);
  });
};

const setStatusWithModel = (queueId, status, { Promise, modelFactory, config }) => {
  return new Promise({ queueId, status }, (res, rej) => {
    if (_.isNil(config.batchServiceFactory.dbName)) throw new Error('batchServiceFactory: Invalid parameters: config.batchServiceFactory.dbName is null or undefined');

    modelFactory
      .get(config.batchServiceFactory.dbName, 'Queue')
      .update({
        id: queueId,
        status
      })
      .then(res)
      .catch(rej);
  });
};

const setStatusWithService = (queueId, status, { Promise, RequestPromise, config }) => {
  return new Promise({ queueId, status }, (res, rej) => {
    if (_.isNil(config.batchServiceFactory.serviceUrl)) throw new Error('batchServiceFactory: Invalid parameters: config.batchServiceFactory.serviceUrl is null or undefined');

    const options = {
      uri: `${config.batchServiceFactory.serviceUrl}/queues/${queueId}`,
      body: { status },
      method: 'put',
      crossedUrl: false,
      json: true,
      isClientAuthenticated: true
    };

    RequestPromise(options)
      .then(res)
      .catch(rej);
  });
};

const addResultWithService = (data, { Promise, RequestPromise, config }) => {
  return new Promise(data, (res, rej) => {
    if (_.isNil(config.batchServiceFactory.serviceUrl)) throw new Error('batchServiceFactory: Invalid parameters: config.batchServiceFactory.serviceUrl is null or undefined');

    const options = {
      uri: `${config.batchServiceFactory.serviceUrl}/queues/${data.queueId}/result`,
      method: 'post',
      body: data,
      crossedUrl: false,
      json: true,
      isClientAuthenticated: true
    };

    RequestPromise(options)
      .then(res)
      .catch(rej);
  });
};

module.exports.init = (message, serviceInit, logger, config, { PromiseFactory, RequestPromiseFactory, ModelFactory }) => {
  const { queueInstanceId, retryCount } = message;
  const Promise = PromiseFactory.init(serviceInit);

  if (config.batchServiceFactory.type === 'model') {
    const modelFactory = ModelFactory.init(serviceInit);

    return {
      addResult: data => addResultWithModel({ ...data, queueId: queueInstanceId, retryCount }, { Promise, modelFactory, config }),
      setStatus: status => setStatusWithModel(queueInstanceId, status, { Promise, modelFactory, config })
    };
  }

  const RequestPromise = RequestPromiseFactory.init(serviceInit);
  return {
    addResult: data => addResultWithService({ ...data, queueId: queueInstanceId, retryCount }, { Promise, RequestPromise, config }),
    setStatus: status => setStatusWithService(queueInstanceId, status, { Promise, RequestPromise, config })
  };
};
