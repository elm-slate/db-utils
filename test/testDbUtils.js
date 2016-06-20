const path = require('path');
const co = require('co');
const bunyan = require('bunyan');
const R = require('ramda');
const is = require('is_js');
const CRC32 = require('crc-32');
const uuid = require('node-uuid');
const program = require('commander');
const dbUtils = require('../lib/index.js');

const logger = bunyan.createLogger({
	name: 'testDbUtils',
	serializers: bunyan.stdSerializers
});

const exit = exitCode => setTimeout(() => process.exit(exitCode), 1000);

process.on('uncaughtException', err => {
	logger.error({err: err}, `Uncaught exception:`);
	exit(1);
});
process.on('unhandledRejection', (reason, p) => {
	logger.error("Unhandled Rejection at: Promise ", p, " reason: ", reason);
	exit(1);
});
process.on('SIGINT', () => {
	logger.info(`SIGINT received.`);
	exit(0);
});
process.on('SIGTERM', () => {
	logger.info(`SIGTERM received.`);
	exit(0);
});

program
	.option('-c, --config-filename <s>', 'configuration file name')
	.option('--dry-run', 'if specified, show run parameters and end')
	.parse(process.argv);

const validateArguments = arguments => {
	var errors = [];
	if (!arguments.configFilename || is.not.string(arguments.configFilename))
		errors = R.append('config-filename is invalid:  ' + arguments.configFilename, errors);
	if (arguments.args.length > 0)
		errors = R.append(`Some command arguments exist after processing command options.  There may be command options after " -- " in the command line.  Unprocessed Command Arguments:  ${program.args}`, errors);
	return errors;
};

const validateConnectionParameters = (parameters, parametersName) => {
	var errors = [];
	if (parameters) {
		if (!parameters.host || is.not.string(parameters.host)) {
			errors = R.append(`${parametersName}.host is missing or invalid:  ${parameters.host}`, errors);
		}
		if (!parameters.databaseName || is.not.string(parameters.databaseName)) {
			errors = R.append(`${parametersName}.databaseName is missing or invalid:  ${parameters.databaseName}`, errors);
		}
		if (parameters.userName && is.not.string(parameters.userName)) {
			errors = R.append(`${parametersName}.userName is invalid:  ${parameters.userName}`, errors);
		}
		if (parameters.password && is.not.string(parameters.password)) {
			errors = R.append(`${parametersName}.password is invalid:  ${parameters.password}`, errors);
		}
	}
	else {
		errors = R.append(`connection parameters for ${parametersName} are missing or invalid`, errors);
	}
	return errors;
};

const logConfig = config => {
	logger.info(`Event Source Connection Params:`, R.pick(['host', 'databaseName', 'user'], config.eventSource));
	if (config.connectTimeout)
		logger.info(`Database Connection Timeout (millisecs):`, config.connectTimeout);
};

const errors = validateArguments(program);
if (errors.length > 0) {
	logger.error('Invalid command line arguments:\n' + R.join('\n', errors));
	program.help();
	process.exit(1);
}
// get absolute name so logs will display absolute path
const configFilename = path.isAbsolute(program.configFilename) ? program.configFilename : path.resolve('.', program.configFilename);

let config;
try {
	logger.info(`${'\n'}Config File Name:  "${configFilename}"${'\n'}`);
	config = require(configFilename);
}
catch (err) {
	logger.error({err: err}, `Exception detected processing configuration file:`);
	process.exit(1);
}

var configErrors = validateConnectionParameters(config.eventSource, 'config.eventsSource');
if (config.connectTimeout) {
	if (!(is.integer(config.connectTimeout) && is.positive(config.connectTimeout))) {
		configErrors = R.append(`config.connectTimeout is invalid:  ${config.connectTimeout}`, configErrors);
	}
}
if (configErrors.length > 0) {
	logger.error(`Invalid configuration parameters:${'\n' + R.join('\n', configErrors)}`);
	program.help();
	process.exit(2);
}

// db connection url
const connectionUrl = dbUtils.createConnectionUrl(config.eventSource);

logConfig(config);


if (program.dryRun) {
	logger.info('dry-run specified, ending program');
	process.exit(0);
}

const maxUIntValue = Math.pow(2, 32);

// convert int to Postgres OID
const intToOid = int => int < 0 ? maxUIntValue + int : int;

const displayEntityCRCs = (entities, pid) => {
	logger.info(R.map(entity => {
		const high = CRC32.str(entity);
		const low = CRC32.str(R.reverse(entity));
		return {entity: entity, connectionPid: pid, CRCs:  {high: high, low: low}, OIDs: {high: intToOid(high), low: intToOid(low)}}}, entities));
};

const displayLockStatus = co.wrap(function *(client) {
	const rows = [];
	const result = yield dbUtils.executeSQLStatement(client, `SELECT * FROM pg_locks WHERE mode = 'ExclusiveLock' AND pid != ${client.processID} ORDER BY pid, virtualtransaction, locktype DESC`);
	R.forEach(row => {
		// classid and objid are Postgres OIDs (unsigned ints) created from each uuid passed to dbUtils.lockEntities
		rows[rows.length] = {
			locktype: row.locktype,
			virtualtransaction: row.virtualtransaction,
			classid: row.classid,
			objid: row.objid,
			pid: row.pid,
			database: row.database,
			mode: row.mode,
			granted: row.granted
		};
	}, result.rows);
	logger.info('Current Lock Status:', rows);
});

const createTestClient = co.wrap(function *(connectionUrl, entities) {
	const client = yield dbUtils.createPooledClient(connectionUrl);
	client.entities = entities;
	logger.info(`client connection ${client.dbClient.processID} created for database ${client.dbClient.database}`);
	return client;
});

const createTestClients = co.wrap(function *(connectionUrl, entitiesList) {
	logger.info(`opening test connections`);
	const clients = [];
	for (var i = 0; i < entitiesList.length; i++) {
		clients[clients.length] = yield createTestClient(connectionUrl, entitiesList[i]);
	}
	return clients;
});

const lockEntities = co.wrap(function *(client, displayClient) {
	try {
		const success = yield dbUtils.lockEntities(client.dbClient, client.entities);
		if (success)
			logger.info(`client connection ${client.dbClient.processID} got locks for Entities ****  "${R.join('", "', client.entities)}"  ****`);
		else
			logger.error(`client connection ${client.dbClient.processID} FAILED to get locks for Entities ****  "${R.join('", "', client.entities)}"  ****`);
		displayEntityCRCs(client.entities, client.dbClient.processID);
		yield displayLockStatus(displayClient);
		return {success: success, client: client};
	}
	catch (err) {
		logger.error(err, `lockEntities failed for client connection ${client.dbClient.processID}`);
		dbUtils.close(client, err);
		throw err;
	}
});

const lockEntitiesForClients = co.wrap(function *(clients, displayClient) {
	const results = [];
	for (var i = 0; i < clients.length; i++) {
		results[results.length] = yield lockEntities(clients[i], displayClient);
	}
	return results;
});

const main = co.wrap(function *(connectionUrl, connectTimeout) {
	let displayClient;
	let clients;
	try {
		dbUtils.setDefaultOptions({logger: logger, connectTimeout: connectTimeout});
		displayClient = yield dbUtils.createClient(connectionUrl);
		logger.info(`Display Connection process id:  ${displayClient.processID}`);
		yield displayLockStatus(displayClient);
		// create uuid entities to be used in the test
		const uuids = [uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4()];
		// create uuid entity GROUPs to use in the test.  each element in the array below contains a GROUP of uuid entities to passed to dbUtils.lockEntities in one call.
		// each call to dbUtils.lockEntities is made on a different test connection so some calls can fail to get advisory transaction locks on the uuid entities if
		// they have already been locked by a previous call on a different connection.  The calls are done on different connections in the order the uuid entity GROUPs
		// are in the array below.
		const entitiesList = [
			// GROUP 0.  lockEntities request for these entities should succeed
			[ uuids[0], uuids[1] ],
			// GROUP 1.  lockEntities request for these entities should succeed
			[ uuids[2] ],
			// GROUP 2.  lockEntities request for these entities should FAIL since at least one of the entities will have been previously locked on
			// a call to lockEntities using another connection (GROUPs 0 and 1)
			[ uuids[1], uuids[2] ],
			// GROUP 3.  lockEntities request for these entities should succeed
			[ uuids[3], uuids[4], uuids[5] ],
			// GROUP 4.  lockEntities request for these entities should FAIL since at least one of the entities will have been previously locked on
			// a call to lockEntities using another connection (GROUP 3)
			[ uuids[6], uuids[4] ]
		];
		clients = yield createTestClients(connectionUrl, entitiesList);
		const results = yield lockEntitiesForClients(clients, displayClient);
		for (var i = 0; i < results.length; i++) {
			if (results[i].success) {
				logger.info(`issuing COMMIT fpr client connection ${results[i].client.dbClient.processID}`);
				yield dbUtils.commit(results[i].client.dbClient);
			}
		}
		logger.info('COMMITs complete');
		yield displayLockStatus(displayClient);
	}
	finally {
		R.forEach(client => {
			if (client) {
				const database = client.dbClient.database;
				const pid = client.dbClient.processID;
				dbUtils.close(client);
				logger.info(`client connection ${pid} closed to database ${database}`);
			}
		}, clients);
		if (displayClient) {
			dbUtils.close(displayClient);
			logger.info('test connections closed');
		}
	}
});

main(connectionUrl, config.connectTimeout)
	.then(() =>  {
		logger.info(`Processing complete`);
		exit(0);
	})
	.catch(err => {
		logger.error({err: err}, `Exception in testDbUtils:`);
		exit(1);
	});
