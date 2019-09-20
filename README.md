# restbase-mod-table-mysql [![Build Status](https://travis-ci.com/femiwiki/restbase-mod-table-mysql.svg?branch=master)](https://travis-ci.com/femiwiki/restbase-mod-table-mysql)

An MySQL back-end module for [RESTBase](https://github.com/wikimedia/restbase)
conforming to the [RESTBase storage
specification](https://github.com/wikimedia/restbase-mod-table-spec).

## Installation

Firstly, install RESTBase. The MySQL back-end module should be pulled in
automatically as a dependency. If you cannot find `restbase-mod-table-mysql` in
RESTBase's `node_modules/` directory, install it using:

```
# TODO
```

## Configuration
Configuration of this module takes place from within an `x-modules` stanza in the YAML-formatted
[RESTBase configuration file](https://github.com/wikimedia/restbase/blob/master/config.example.wikimedia.yaml).
While complete configuration of RESTBase is beyond the scope of this document, (see the
[RESTBase docs](https://github.com/wikimedia/restbase) for that), this section covers the
[restbase-mod-table-cassandra](https://github.com/wikimedia/restbase-mod-table-cassandra) specifics.

```yaml
/{module:table}:
  x-modules:
    - name: restbase-mod-table-mysql
      version: 1.0.0
      type: npm
      options:
        conf:
          host: localhost
          database: restbase
          username: mysql
          password: mysql
          show_sql: false
```
