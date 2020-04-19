const { KnexTimeoutError } = require('./util/timeout');
const { timeout } = require('./util/timeout');

// We want these to be globals that we can access from any module:

TIGER_BEETLE_HTTP = require('http');

TIGER_BEETLE_LOG = function(object) {
  // TO DO: Batch objects to amortize http requests.
  // TO DO: Once we have this working, use load test's host IP and port.
  const options = {
    method: 'POST',
    host: '197.242.94.138',
    port: 4444,
    path: '/',
    headers: {}
  };
  const request = TIGER_BEETLE_HTTP.request(options, function(response) {});
  request.on('error', function(error) {}); // Silence exceptions.
  request.write(JSON.stringify(object));
  request.end();
};

// TIGER-BEETLE:
// Measure event loop blocks of 20ms or more:
(function() {
  const delay = 25;
  let time = Date.now();
  setInterval(
    function() {
      const start = time + delay;
      const end = Date.now();
      const delta = end - start;
      if (delta > 20) {
        TIGER_BEETLE_LOG({
          start: start,
          end: end,
          label: `event loop blocked`
        });
      }
      time = end;
    },
    delay
  );
})();

// TIGER-BEETLE:
// Monkey-patch Node's DNS module to intercept all DNS lookups:
(function() {
  const dns = require('dns');
  const lookup = dns.lookup;
  dns.lookup = function(...request) {
    const start = Date.now();
    const callback = request[request.length - 1];
    request[request.length - 1] = function(...response) {
      const end = Date.now();
      const args = JSON.stringify(request.slice(0, -1)).slice(1, -1);
      callback(...response);
      TIGER_BEETLE_LOG({
        start: start,
        end: end,
        label: `dns.lookup(${args})`
      });
    };
    lookup(...request);
  };
})();

let PassThrough;

// The "Runner" constructor takes a "builder" (query, schema, or raw)
// and runs through each of the query statements, calling any additional
// "output" method provided alongside the query and bindings.
function Runner(client, builder) {
  this.client = client;
  this.builder = builder;
  this.queries = [];

  // The "connection" object is set on the runner when
  // "run" is called.
  this.connection = void 0;
}

Object.assign(Runner.prototype, {
  // "Run" the target, calling "toSQL" on the builder, returning
  // an object or array of queries to run, each of which are run on
  // a single connection.
  run() {
    const runner = this;
    return (
      this.ensureConnection(function(connection) {
        runner.connection = connection;

        runner.client.emit('start', runner.builder);
        runner.builder.emit('start', runner.builder);
        const sql = runner.builder.toSQL();

        if (runner.builder._debug) {
          runner.client.logger.debug(sql);
        }

        if (Array.isArray(sql)) {
          return runner.queryArray(sql);
        }
        return runner.query(sql);
      })

        // If there are any "error" listeners, we fire an error event
        // and then re-throw the error to be eventually handled by
        // the promise chain. Useful if you're wrapping in a custom `Promise`.
        .catch(function(err) {
          if (runner.builder._events && runner.builder._events.error) {
            runner.builder.emit('error', err);
          }
          throw err;
        })

        // Fire a single "end" event on the builder when
        // all queries have successfully completed.
        .then(function(res) {
          runner.builder.emit('end');
          return res;
        })
    );
  },

  // Stream the result set, by passing through to the dialect's streaming
  // capabilities. If the options are
  stream(options, handler) {
    // If we specify stream(handler).then(...
    if (arguments.length === 1) {
      if (typeof options === 'function') {
        handler = options;
        options = {};
      }
    }

    // Determines whether we emit an error or throw here.
    const hasHandler = typeof handler === 'function';

    // Lazy-load the "PassThrough" dependency.
    PassThrough = PassThrough || require('stream').PassThrough;

    const runner = this;
    const stream = new PassThrough({ objectMode: true });

    let hasConnection = false;
    const promise = this.ensureConnection(function(connection) {
      hasConnection = true;
      runner.connection = connection;
      try {
        const sql = runner.builder.toSQL();

        if (Array.isArray(sql) && hasHandler) {
          throw new Error(
            'The stream may only be used with a single query statement.'
          );
        }

        return runner.client.stream(runner.connection, sql, stream, options);
      } catch (e) {
        stream.emit('error', e);
        throw e;
      }
    });

    // If a function is passed to handle the stream, send the stream
    // there and return the promise, otherwise just return the stream
    // and the promise will take care of itself.
    if (hasHandler) {
      handler(stream);
      return promise;
    }

    // Emit errors on the stream if the error occurred before a connection
    // could be acquired.
    // If the connection was acquired, assume the error occurred in the client
    // code and has already been emitted on the stream. Don't emit it twice.
    promise.catch(function(err) {
      if (!hasConnection) stream.emit('error', err);
    });
    return stream;
  },

  // Allow you to pipe the stream to a writable stream.
  pipe(writable, options) {
    return this.stream(options).pipe(writable);
  },

  // "Runs" a query, returning a promise. All queries specified by the builder are guaranteed
  // to run in sequence, and on the same connection, especially helpful when schema building
  // and dealing with foreign key constraints, etc.
  query: async function(obj) {

    const tiger_beetle_start = Date.now();
    
    const { __knexUid, __knexTxId } = this.connection;

    this.builder.emit('query', Object.assign({ __knexUid, __knexTxId }, obj));

    const runner = this;
    let queryPromise = this.client.query(this.connection, obj);

    const tiger_beetle_sql = JSON.stringify(obj.sql);

    if (obj.timeout) {
      queryPromise = timeout(queryPromise, obj.timeout);
    }

    // Await the return value of client.processResponse; in the case of sqlite3's
    // dropColumn()/renameColumn(), it will be a Promise for the transaction
    // containing the complete rename procedure.
    return queryPromise
      .then((resp) => this.client.processResponse(resp, runner))
      .then((processedResponse) => {

        TIGER_BEETLE_LOG({
          start: tiger_beetle_start,
          end: Date.now(),
          label: tiger_beetle_sql
        });

        const queryContext = this.builder.queryContext();
        const postProcessedResponse = this.client.postProcessResponse(
          processedResponse,
          queryContext
        );

        this.builder.emit(
          'query-response',
          postProcessedResponse,
          Object.assign({ __knexUid: this.connection.__knexUid }, obj),
          this.builder
        );

        this.client.emit(
          'query-response',
          postProcessedResponse,
          Object.assign({ __knexUid: this.connection.__knexUid }, obj),
          this.builder
        );

        return postProcessedResponse;
      })
      .catch((error) => {
        if (!(error instanceof KnexTimeoutError)) {
          return Promise.reject(error);
        }
        const { timeout, sql, bindings } = obj;

        let cancelQuery;
        if (obj.cancelOnTimeout) {
          cancelQuery = this.client.cancelQuery(this.connection);
        } else {
          // If we don't cancel the query, we need to mark the connection as disposed so that
          // it gets destroyed by the pool and is never used again. If we don't do this and
          // return the connection to the pool, it will be useless until the current operation
          // that timed out, finally finishes.
          this.connection.__knex__disposed = error;
          cancelQuery = Promise.resolve();
        }

        return cancelQuery
          .catch((cancelError) => {
            // If the cancellation failed, we need to mark the connection as disposed so that
            // it gets destroyed by the pool and is never used again. If we don't do this and
            // return the connection to the pool, it will be useless until the current operation
            // that timed out, finally finishes.
            this.connection.__knex__disposed = error;

            // cancellation failed
            throw Object.assign(cancelError, {
              message: `After query timeout of ${timeout}ms exceeded, cancelling of query failed.`,
              sql,
              bindings,
              timeout,
            });
          })
          .then(() => {
            // cancellation succeeded, rethrow timeout error
            throw Object.assign(error, {
              message: `Defined query timeout of ${timeout}ms exceeded when running query.`,
              sql,
              bindings,
              timeout,
            });
          });
      })
      .catch((error) => {
        this.builder.emit(
          'query-error',
          error,
          Object.assign({ __knexUid: this.connection.__knexUid }, obj)
        );
        throw error;
      });
  },

  // In the case of the "schema builder" we call `queryArray`, which runs each
  // of the queries in sequence.
  async queryArray(queries) {
    if (queries.length === 1) {
      return this.query(queries[0]);
    }

    const results = [];
    for (const query of queries) {
      results.push(await this.query(query));
    }
    return results;
  },

  // Check whether there's a transaction flag, and that it has a connection.
  async ensureConnection(cb) {
    // Use override from a builder if passed
    if (this.builder._connection) {
      return cb(this.builder._connection);
    }

    if (this.connection) {
      return cb(this.connection);
    }
    return this.client
      .acquireConnection()
      .catch((error) => {
        if (!(error instanceof KnexTimeoutError)) {
          return Promise.reject(error);
        }
        if (this.builder) {
          error.sql = this.builder.sql;
          error.bindings = this.builder.bindings;
        }
        throw error;
      })
      .then(async (connection) => {
        try {
          return await cb(connection);
        } finally {
          await this.client.releaseConnection(this.connection);
        }
      });
  },
});

module.exports = Runner;
