const pg = require('pg');
const co = require('co');
const QueryStream = require('pg-query-stream');
const is = require('is_js');
const R = require('ramda');

const _private = {
	logger: null,
	queryStreamOptions: {
		highWaterMark: 16 * 1024,
		batchSize: 10000
	},
	connectTimeout: 15000
};

const logError = (err, message) => {
	if (_private.logger) {
		message = message || '';
		_private.logger.error({err: err}, message);
	}
};

const getHostAndDb = connectionUrl => {
	// host is in first capturing group and database is in second capturing group
	const hostAndDb	= /@([\w\d.]+)\/([\w\d]+)/.exec(connectionUrl);
	if (hostAndDb && hostAndDb.length === 3) {
		return {host: hostAndDb[1], database: hostAndDb[2]};
	}
	else {
		return {host: 'n/a', database: 'n/a'};
	}
};

const dbUtils = {
	setDefaultOptions:  options => {
		if (options) {
			_private.logger = options.logger;
			if (options.highWaterMark && is.integer(options.highWaterMark) && is.positive(options.highWaterMark)) {
				_private.queryStreamOptions.highWaterMark = options.highWaterMark;
			}
			if (options.batchSize && is.integer(options.batchSize) && is.positive(options.batchSize)) {
				_private.queryStreamOptions.batchSize = options.batchSize;
			}
			if (options.connectTimeout && is.integer(options.connectTimeout) && is.positive(options.connectTimeout)) {
				_private.connectTimeout = options.connectTimeout;
			}
		}
	},
	createConnectionUrl: connectionParams => {
		const scheme = 'postgres';
		if (connectionParams.user) {
			if (connectionParams.password) {
				return `${scheme}://${connectionParams.user}:${connectionParams.password}@${connectionParams.host}/${connectionParams.databaseName}`;
			}
			else {
				return `${scheme}://${connectionParams.user}@${connectionParams.host}/${connectionParams.databaseName}`;
			}
		}
		else {
			return `${scheme}://${connectionParams.host}/${connectionParams.databaseName}`;
		}
	},
	createClient: conString => {
		var client = new pg.Client(conString);
		// wrap asynchronous callback in promise
		return new Promise((resolve, reject) => {
			const hostAndDb = getHostAndDb(conString);
			const timer = setTimeout(() => {
				const error = new Error(`connect to database "${hostAndDb.database}" on host "${hostAndDb.host}" failed.  Error:  Connect timeout after ${_private.connectTimeout} millisec`);
				reject(error);
			}, _private.connectTimeout);
			client.connect(err => {
				// need to handle errors in callback function since called asynchronously
				try {
					clearTimeout(timer);
					if (err) {
						logError(new Error(`connect to database "${hostAndDb.database}" on host "${hostAndDb.host}" failed.  Error:  ${err.message}`));
						reject(err);
					}
					else {
						resolve(client);
					}
				}
				catch(err) {
					reject(err);
				}
			});
		});
	},
	createPooledClient: conString => {
		return new Promise((resolve, reject) => {
			const hostAndDb = getHostAndDb(conString);
			const timer = setTimeout(() => {
				const error = new Error(`connect to connection pool for database "${hostAndDb.database}" on host "${hostAndDb.host}" failed.` +
					`  Error:  Connect timeout after ${_private.connectTimeout} millisec`);
				reject(error);
			}, _private.connectTimeout);
			pg.connect(conString, (err, client, done) => {
				try {
					clearTimeout(timer);
					if (err) {
						logError(new Error(`connect to connection pool for database "${hostAndDb.database}" on host "${hostAndDb.host}" failed.  Error:  ${err.message}`));
						reject(err);
					}
					else {
						resolve({dbClient: client, releaseClient: done});
					}
				}
				catch(err) {
					reject(err);
				}
			});
		});
	},
	createQueryStream: (client, statement, prepareStmtParams, options) => {
		const optionsCopy = R.pick(['highWaterMark', 'batchSize'], R.merge(options, _private.queryStreamOptions));
		return client.query(new QueryStream(statement, prepareStmtParams, optionsCopy))
	},
	executeSQLStatement: (client, statement, prepareStmtParams) => {
		prepareStmtParams = prepareStmtParams || [];
		return new Promise((resolve, reject) => {
			client.query(statement, prepareStmtParams, (err, result) => {
				try {
					if (err) {
						logError(new Error(`query failed:  "${statement.substr(0, 200)}"... for database (${client.database}).  Error:  ${err.message}`));
						reject(err);
					}
					else {
						resolve(result);
					}
				}
				catch(err) {
					reject(err);
				}
			});
		});
	},
	close: client => {
		if (client.releaseClient) {
			client.releaseClient(client.dbClient);
		}
		else {
			client.end();
		}
	}
};

module.exports = dbUtils;

