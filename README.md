# slate-db-utils
Postgresql database utilities for Slate

Provides `Postgresql` database connection and SQL statement processing functionality for Slate.

## Installation
> npm install @panosoft/slate-db-utils

## API

- [`close`](#close)
- [`createClient`](#createClient)
- [`commit`](#commit)
- [`createConnectionUrl`](#createConnectionUrl)
- [`createPooledClient`](#createPooledClient)
- [`createQueryStream`](#createQueryStream)
- [`executeSQLStatement`](#executeSQLStatement)
- [`lockEntities`](#lockEntities)
- [`rollback`](#rollback)
- [`setDefaultOptions`](#setDefaultOptions)

---

<a name="close"></a>
### close (client)

Closes a client connection to a `Postgresql` database created with either `createClient` or `createPooledClient`.

#### Arguments

- `client` - The object returned from calling [`createClient`](#createClient) or [`createPooledClient`](#createPooledClient).
- `err` - optional parameter for clients created with `createPooledClient`.
  - if the parameter is missing or `falsy`, then client connection will be destroyed and not returned to the connection pool.
  - if the parameter is `truthy` the client connection will be returned to the connection pool.
  - do not return client connections to the connection pool while they are in the middle of a transaction.  unpredictable results will occur the next time the connection is used.
  - this parameter has no effect for closing clients created with `createClient`.

#### Example

``` javascript
const co = require('co');
const dbUtils = require('@panosoft/slate-db-utils');

const exampleForClient = co.wrap(function *(connectionParams) {
  const dbClient = yield dbUtils.createClient(dbUtils.createConnectionUrl(connectionParams));
  dbUtils.close(dbClient);
});

const exampleForPooledClient = co.wrap(function *(connectionParams) {
  let pooledClient;
  try {
    pooledClient = yield dbUtils.createPooledClient(dbUtils.createConnectionUrl(connectionParams));
    // closing the pooled connection using this form of close will return the client connection to the connection pool.  make the sure connection is not in the middle of
    // a transaction or future results using the same connection will be unpredictable.
    dbUtils.close(pooledClient);
  }
  catch (err) {
    if (pooledClient) {
      // closing pooled client connection with truthy err parameter will destroy the client connection and not return the connection to the connection pool
      dbUtils.close(pooledClient, err);
    }
  }
});

const example = co.wrap(function *(connectionParams) {
  yield exampleForClient(connectionParams);
  yield exampleForPooledClient(connectionParams);
});

example({host: 'exampleHost', databaseName: 'exampleDbname', user: 'exampleUser', password: 'examplePassword'})
.then(() =>  {
  console.log(`Example complete`);
  process.exit(0);
})
.catch(err => {
  console.error(err.stack || err.message);
  process.exit(1);
});
```

<a name="commit"></a>
### commit (client)

Commits a transaction.

#### Arguments

- `client` - A [`pg`](https://github.com/brianc/node-postgres) Client.

#### Example

See [`lockEntities`](#lockEntities).

<a name="createClient"></a>
### createClient (conString)

Creates a [`pg`](https://github.com/brianc/node-postgres) Client using a `Postgresql` connection URL string.

Returns a Promise that is resolved with a [`pg`](https://github.com/brianc/node-postgres) Client.

#### Arguments

- `conString` - A `Postgresql` connection URL string.

#### Example

See [`close`](#close).

<a name="createConnectionUrl"></a>
### createConnectionUrl (connectionParams)

Creates a `Postgresql` connection URL string from connection parameters.

Returns a `Postgresql` connection URL string.

#### Arguments

- `connectionParams` - A `Postgresql` connection parameters object with the following keys:
  - `host` - the name of the server
  - `databaseName` - name of the database
  - `user` - the user (IFF database requires)
  - `password` - password (IFF database requires)
  - `connectTimeout` - number of milliseconds to wait for a connection (OPTIONAL defaults: `15000`)

#### Example

See [`close`](#close).

<a name="createPooledClient"></a>
### createPooledClient (conString)

Creates an object containing a `dbClient` property which is a [`pg`](https://github.com/brianc/node-postgres) Client.  The `pg` Client object is retrieved from a connection pool built using the supplied `Postgresql` connection URL string.

Returns a Promise that is resolved with an object containing a [`pg`](https://github.com/brianc/node-postgres) Client (property `dbClient`) retrieved from a connection pool.

#### Arguments

- `conString` - A `Postgresql` connection URL string.

#### Example

See [`close`](#close).

<a name="createQueryStream"></a>
### createQueryStream (client, statement, prepareStmtParams, options)

Creates a readable stream of rows returned from the input SQL statement.

Returns a readable stream of rows.

#### Arguments

- `client` - A [`pg`](https://github.com/brianc/node-postgres) Client.
- `statement` - A SQL statement with or without parameters, i.e. using $1, $2, etc.
- `prepareStmtParams` - An array of prepared statement parameters.  Required if the SQL statement contains parameters.
- `options` - An optional object that has properties used by the QueryStream constructor in the [`pg-query-stream`](https://github.com/brianc/node-pg-query-stream) library which contain the following keys:
  - `highWaterMark (default 16384)` - the node Readable Stream [`highWaterMark`](https://nodejs.org/api/stream.html#stream_new_stream_readable_options)
  - `batchSize (default 10000)` - the number of rows that will be retrieved from the database server for each request to resupply the stream

#### Example

``` javascript
const co = require('co');
const coread = require('co-read');
const dbUtils = require('@panosoft/slate-db-utils');

const getRowsFromStream = co.wrap(function *(rowStream) {
  var rows = [];
  var endOfStream = false;
  while (!endOfStream) {
    var row = yield coread(rowStream);
    // event returned
    if (row) {
      rows[rows.length] = row;
    }
    // end of stream
    else {
      endOfStream = true;
    }
  }
  return rows;
});

const example = co.wrap(function *(connectionParams) {
  const pooledClient = yield dbUtils.createPooledClient(dbUtils.createConnectionUrl(connectionParams));
  const rowStream = dbUtils.createQueryStream(pooledClient.dbClient, 'SELECT * FROM exampleTable');
  rowStream.on('error', err => {
    console.error({err: err});
    throw err;
  });
  const rows = yield getRowsFromStream(rowStream);
  dbUtils.close(pooledClient);
});

example({host: 'exampleHost', databaseName: 'exampleDbname', user: 'exampleUser', password: 'examplePassword'})
.then(() =>  {
  console.log(`Example complete`);
  process.exit(0);
})
.catch(err => {
  console.error(err.stack || err.message);
  process.exit(1);
});
```

<a name="executeSQLStatement"></a>
### executeSQLStatement (client, statement, prepareStmtParams)

Executes a SQL statement.

Returns a Promise that is resolved with a [`pg`](https://github.com/brianc/node-postgres) Result object.

#### Arguments

- `client` - A [`pg`](https://github.com/brianc/node-postgres) Client.
- `statement` - A SQL statement with or without parameters, i.e. using $1, $2, etc.
- `prepareStmtParams` - An array of prepared statement parameters.  Required if the SQL statement contains parameters.

#### Example

``` javascript
const co = require('co');
const dbUtils = require('@panosoft/slate-db-utils');

const exampleForClient = co.wrap(function *(connectionParams) {
  const dbClient = yield dbUtils.createClient(dbUtils.createConnectionUrl(connectionParams));
  const results = yield dbUtils.executeSQLStatement(dbClient, 'SELECT * FROM exampleTable');
  results.rows.forEach((row, i) => {
    console.log(`Row ${i}:  ${row.column1}, ${row.column2}`);
  });
  dbUtils.close(dbClient);
  return results.rowCount;
});

const exampleForPooledClient = co.wrap(function *(connectionParams) {
  const pooledClient = yield dbUtils.createPooledClient(dbUtils.createConnectionUrl(connectionParams));
  const results = yield dbUtils.executeSQLStatement(pooledClient.dbClient, 'SELECT * FROM exampleTable');
  results.rows.forEach((row, i) => {
    console.log(`Row ${i}:  ${row.column1}, ${row.column2}`);
  });
  dbUtils.close(pooledClient);
});

const example = co.wrap(function *(connectionParams) {
  const clientRowCount = yield exampleForClient(connectionParams);
  const pooledClientRowCount = yield exampleForPooledClient(connectionParams);
});

example({host: 'exampleHost', databaseName: 'exampleDbname', user: 'exampleUser', password: 'examplePassword'})
.then(() =>  {
  console.log(`Example complete`);
  process.exit(0);
})
.catch(err => {
  console.error(err.stack || err.message);
  process.exit(1);
});
```

<a name="lockEntities"></a>
### lockEntities (client, entityIds)

Creates a transaction and exclusively locks entities from access by other cooperative parties using Postgresql advisory transaction locks derived from each entity's uuid. Cooperative parties are also using this function before writing to the Event Source database. Slate requires that all writes first use this function.

#### Arguments

- `client` - A [`pg`](https://github.com/brianc/node-postgres) Client.
- `entityIds` - An array of uuids of the entities to lock.

#### Example

``` javascript
const co = require('co');
const dbUtils = require('@panosoft/slate-db-utils');

const close = (client, err) => {
  try {
    if (client)
      dbUtils.close(client, err);
  }
  catch (err) {
    console.error(err.stack || err.message);
  }
};
const exampleForClient = co.wrap(function *(connectionParams) {
  let dbClient;
  try {
    dbClient = yield dbUtils.createClient(dbUtils.createConnectionUrl(connectionParams));
    const locksObtained = yield dbUtils.lockEntities(dbClient, ['06f1ee30-a2f5-4585-9bc7-3c78e32075b9', '2dce4c44-3af8-4c5a-bff8-7ac7cca443e0']);
    if (locksObtained) {
      // do some work you want to abort
      // rollback transaction started by lockEntities
      yield dbUtils.rollback(dbClient);
    }
    close(dbClient);
  }
  catch (err) {
    // closing client will destroy the client connection so no need to do rollback if locks were obtained and rollback was not done
    close(dbClient);
    throw err;
  }
});

const exampleForPooledClient = co.wrap(function *(connectionParams) {
  let pooledClient;
  try {
    pooledClient = yield dbUtils.createPooledClient(dbUtils.createConnectionUrl(connectionParams));
    const locksObtained = yield dbUtils.lockEntities(pooledClient.dbClient, ['06f1ee30-a2f5-4585-9bc7-3c78e32075b9', '2dce4c44-3af8-4c5a-bff8-7ac7cca443e0']);
    if (locksObtained) {
      // do some work
      // commit transaction started by dbUtils.lockEntities
      yield dbUtils.commit(pooledClient.dbClient);
    }
    // closing pooledClient without err parameter will return client connection to connection pool
    close(pooledClient);
  }
  catch (err) {
    // closing pooledClient with err parameter will destroy client connection and not return it to the connection pool so no need to do rollback if locks were obtained and
    // commit was not done
    close(pooledClient, err);
    throw err;
  }
});

const example = co.wrap(function *(connectionParams) {
  yield exampleForClient(connectionParams);
  yield exampleForPooledClient(connectionParams);
});

example({host: 'exampleHost', databaseName: 'exampleDbname', user: 'exampleUser', password: 'examplePassword'})
.then(() =>  {
  console.log(`Example complete`);
  process.exit(0);
})
.catch(err => {
  console.error(err.stack || err.message);
  process.exit(1);
});
```

<a name="rollback"></a>
### rollback (client)

Aborts a transaction.

#### Arguments

- `client` - A [`pg`](https://github.com/brianc/node-postgres) Client.

#### Example

See [`lockEntities`](#lockEntities).

<a name="setDefaultOptions"></a>
### setDefaultOptions (options)

Sets defaults options for `dbUtils`.

#### Arguments

- `options` - An options object containing logger, highWaterMark, batchSize and/or connectTimeout properties.
 - `logger` - A logger that supports `info` and `error` functions, e.g. [`bunyan`](https://github.com/trentm/node-bunyan).  Default is no log message generated if dbUtils detects an error or has an info message to display.
 - `highWaterMark` - Same as in `options` parameter in [`createQueryStream`](#createQueryStream).
 - `batchSize` - Same as in `options` parameter in [`createQueryStream`](#createQueryStream).
 - `connectTimeout` - Same as in `connectionParams` parameter in [`createConnectionUrl`](#createConnectionUrl).

#### Example

 ``` javascript
const co = require('co');
const dbUtils = require('@panosoft/slate-db-utils');

const example = co.wrap(function *(options) {
  dbUtils.setDefaultOptions(options);
});

example({logger: null, highWaterMark: 32 * 1024, batchSize: 20000, connectTimeout: 30000})
.then(() =>  {
  console.log(`Example complete`);
  process.exit(0);
})
.catch(err => {
  console.error(err.stack || err.message);
  process.exit(1);
});
```
