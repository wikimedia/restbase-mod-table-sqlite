# restbase-mod-table-sqlite [![Build Status](https://travis-ci.org/wikimedia/restbase-mod-table-sqlite.svg?branch=master)](https://travis-ci.org/wikimedia/restbase-mod-table-sqlite)

An SQLite3 back-end module for [RESTBase](https://github.com/wikimedia/restbase)
conforming to the [RESTBase storage
specification](https://github.com/wikimedia/restbase-mod-table-spec).

## Installation

Firstly, install RESTBase. The SQLite back-end module should be pulled in
automatically as a dependency. If you cannot find `restbase-mod-table-sqlite` in
RESTBase's `node_modules/` directory, install it using:

```
npm install restbase-mod-table-sqlite
```

Note: in order to successfully install the module, you are going to need the
SQLite3 development headers.

## Configuration

RESTBase comes pre-configured to use Cassandra as its back-end storage. In order
to select SQLite, RESTBase's [`table` module in the configuration
file](https://github.com/wikimedia/restbase/blob/58d0d733fcf1bd625a20cfcdf67b9cdce5e0ca13/config.example.yaml#L53)
needs to be instructed to use this module; simply replace
`restbase-mod-table-cassandra` with `restbase-mod-table-sqlite`. The table that
follows lists the configuration options accepted by this module.

Option | Default | Description
------ | ------- | -----------
`dbname` | `restbase` | The path to the database file
`pool_idle_timeout` | `10000` | The amount of milliseconds a connection to the database is kept open during idle periods
`retry_delay` | `100` | The amount of time (in ms) to wait before retrying queries when the database is locked
`retry_limit` | `5` | The maximum number of times a query is retried
`show_sql` | `false` | Whether to log queries being executed; for debugging purposes only

All of the configuration directives are optional. Here's an example of the
`table` module using the SQLite back-end module:

```yaml
/{module:table}:
  x-modules:
    - name: restbase-mod-table-sqlite
      version: 1.0.0
      type: npm
      options:
        conf:
          dbname: /var/lib/restbase/db.sqlite3
          pool_idle_timeout: 20000
          retry_delay: 250
          retry_limit: 10
          show_sql: false
```

