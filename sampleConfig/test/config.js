var config = {
	// optional parameter.  database connection timeout in milliseconds.  default value:  15000.
	connectTimeout: 5000,
	// connection parameters to event source database.
	eventSource: {
		host: 'localhost',
		databaseName: 'sourceDb',
		user: 'user',
		password: 'password'
	}
};

module.exports = config;