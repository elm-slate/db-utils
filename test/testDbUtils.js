const path = require('path');
const co = require('co');
const bunyan = require('bunyan');
const R = require('ramda');
const is = require('is_js');
const CRC32 = require('crc-32');
const uuid = require('node-uuid');
const program = require('commander');
const cRu = require('@panosoft/co-ramda-utils');
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
/////////////////////////////////////////////////////////////////////////////////////
//  validate configuration
/////////////////////////////////////////////////////////////////////////////////////
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
/////////////////////////////////////////////////////////////////////////////////////
//  to reach this point, configuration must be valid and --dry-run was not specified
/////////////////////////////////////////////////////////////////////////////////////
const maxUIntValue = Math.pow(2, 32);

// convert int to Postgres OID
const intToOid = int => int < 0 ? maxUIntValue + int : int;

const displayEntityCRCs = (entityIds, pid) => {
	logger.info(R.map(entityId => {
		const high = CRC32.str(entityId);
		const low = CRC32.str(R.reverse(entityId));
		return {entityId: entityId, connectionPid: pid, CRCs:  {high: high, low: low}, OIDs: {high: intToOid(high), low: intToOid(low)}}}, entityIds));
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

const createTestClient = co.wrap(function *(connectionUrl, entitiesTest) {
	const client = yield dbUtils.createPooledClient(connectionUrl);
	client.__slateEntityIds = entitiesTest.entityIds;
	client.__slateTestResult = entitiesTest.shouldSucceed === true ? 'succeed' : 'FAIL';
	logger.info(`client connection ${client.dbClient.processID} created for database ${client.dbClient.database}.  get locks attempt should ${client.__slateTestResult} for this client`);
	return client;
});

const createTestClients = co.wrap(function *(connectionUrl, entitiesList) {
	logger.info(`opening test connections`);
	const clients = [];
	yield cRu.forEachG(function *(entitiesTest) {
		clients[clients.length] = yield createTestClient(connectionUrl, entitiesTest);
	}, entitiesList);
	return clients;
});

const lockEntities = co.wrap(function *(client, displayClient) {
	try {
		logger.info(`client connection ${client.dbClient.processID} should ${client.__slateTestResult} getting locks for entityIds ****  "${R.join('", "', client.__slateEntityIds)}"  ****`);
		const success = yield dbUtils.lockEntities(client.dbClient, client.__slateEntityIds);
		if (success)
			logger.info(`client connection ${client.dbClient.processID} succeeded getting locks for entityIds ****       "${R.join('", "', client.__slateEntityIds)}"  ****`);
		else
			logger.error(`client connection ${client.dbClient.processID} FAILED to get locks for entityIds ****         "${R.join('", "', client.__slateEntityIds)}"  ****`);
		displayEntityCRCs(client.__slateEntityIds, client.dbClient.processID);
		yield displayLockStatus(displayClient);
		return {success: success, client: client};
	}
	catch (err) {
		logger.error(err, `lockEntities failed for client connection ${client.dbClient.processID}`);
		dbUtils.close(client, err);
		yield displayLockStatus(displayClient);
		return {success: false, client: client};
	}
});

const lockEntitiesForClients = co.wrap(function *(clients, displayClient) {
	const results = [];
	yield cRu.forEachG(function *(client) {
		results[results.length] = yield lockEntities(client, displayClient);
	}, clients);
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
		// create uuid entityIds and expected outcome for each lock attempt in the test
		const uuids = [uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4(), uuid.v4()];
		// create test GROUPs to use in the testing.  each element in the array below contains a GROUP of uuid entityIds to be passed to dbUtils.lockEntities in one call
		// along with the expect outcome of the call.  each call to dbUtils.lockEntities is made on a different test connection so some calls can fail to get
		// advisory transaction locks on the uuid entityIds if  they have already been locked by a previous call on a different connection.  The calls are done on
		// different connections in the order the test GROUPs are in the array below (e.g. element 0, 1, 2...).
		const entitiesList = [
			// GROUP 0.  lockEntities request for these entityIds should succeed
			{ entityIds: [ uuids[0], uuids[1] ], shouldSucceed: true},
			// GROUP 1.  lockEntities request for these entityIds should succeed
			{ entityIds: [ uuids[2] ], shouldSucceed: true},
			// GROUP 2.  lockEntities request for these entityIds should FAIL since at least one of the entityIds will have already been locked by
			// a call to lockEntities in a previous connection (GROUPs 0 and 1)
			{ entityIds: [ uuids[1], uuids[2] ], shouldSucceed: false},
			// GROUP 3.  lockEntities request for these entityIds should succeed
			{ entityIds: [ uuids[3], uuids[4], uuids[5] ], shouldSucceed: true},
			// GROUP 4.  lockEntities request for these entityIds should FAIL since at least one of the entityIds will have already been locked by
			// a call to lockEntities in a previous connection (GROUP 3)
			{ entityIds: [ uuids[6], uuids[4] ], shouldSucceed: false},
			// GROUP 5.  lockEntities request for these entityIds should FAIL and throw an Error since no entityIds exist
			{ entityIds: [], shouldSucceed: false}
		];
		clients = yield createTestClients(connectionUrl, entitiesList);
		const results = yield lockEntitiesForClients(clients, displayClient);
		yield cRu.forEachG(function *(result) {
			if (result.success) {
				logger.info(`issuing COMMIT fpr client connection ${result.client.dbClient.processID}`);
				yield dbUtils.commit(result.client.dbClient);
			}
		}, results);
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
