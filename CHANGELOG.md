## [1.2.0](https://github.com/castoredc/avrocurio/tree/v1.2.0) - 2026-01-28

### New Features

- Add an in-memory schema-registry client

  An in-memory implementation of the schema registry client has been added which can be used for development and testing purposes, removing the need to run a live instance of Apicurio. ([#29](https://github.com/castoredc/avrocurio/issues/29))


## [1.1.0](https://github.com/castoredc/avrocurio/tree/v1.1.0) - 2025-10-13

### Changes

- Add exception hint about `str` types when `deserialize` fails

  The `message` parameter of the `deserialize` method takes `bytes`, as it expects an Avro binary wrapped in Confluent Registry framing.

  Due to Python's duck typing, a `str` can be passed in without type checkers complaining, but this can lead to subtle and confusing errors if that `str` is the result of an inadvertent `decode()` on the Avro bytes.

  When this happens, a hint is now added to the raised `DeserializationError` to help users identify this problem more easily.

  This does require bumping the minimum supported Python version to 3.11 (up from the previous minimum supported version 3.10), as `BaseException.add_note` was only added in Python 3.11.


## [1.0.0](https://github.com/castoredc/avrocurio/tree/v1.0.0) - 2025-07-31

Initial public release.
