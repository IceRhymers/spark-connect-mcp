# Changelog

## [0.2.0](https://github.com/IceRhymers/spark-connect-mcp/compare/v0.1.0...v0.2.0) (2026-04-18)


### Features

* add force param and set_preflight_threshold for preflight integration (Issue [#39](https://github.com/IceRhymers/spark-connect-mcp/issues/39)) ([6caeb7b](https://github.com/IceRhymers/spark-connect-mcp/commit/6caeb7b1c43cc435bf5ec37feafbd70b5ac9b122))
* add list_dataframes and drop_dataframe tool module ([fb241e5](https://github.com/IceRhymers/spark-connect-mcp/commit/fb241e55cc1fa874408afb71de33ae479350a4c5))
* add list_dataframes and drop_dataframe tools for handle management ([e25cf4c](https://github.com/IceRhymers/spark-connect-mcp/commit/e25cf4c3ed24a7b78ed94b454fed6e089580dcd4))
* add origin strings to all register() calls in lazy tools ([a34ec52](https://github.com/IceRhymers/spark-connect-mcp/commit/a34ec52ee7c25d67683fcf6a514ba2fec5e24e50))
* add RegisteredFrame dataclass and metadata tracking to DataFrameRegistry ([9f9aec1](https://github.com/IceRhymers/spark-connect-mcp/commit/9f9aec11b3e89c7ff1c18e210238dc0649f22266))
* add release-please automation ([1cba43a](https://github.com/IceRhymers/spark-connect-mcp/commit/1cba43a554e9dced23faa9e1b3cb64ea76644002))
* add release-please automation ([4d76236](https://github.com/IceRhymers/spark-connect-mcp/commit/4d76236cb7c0f5b77e5cc16fa6a0b5711503f039))
* implement preflight size check with CBO confidence tiers (Issue [#39](https://github.com/IceRhymers/spark-connect-mcp/issues/39)) ([cb635ba](https://github.com/IceRhymers/spark-connect-mcp/commit/cb635ba5b68167d499be4532c652b317c75bd845))
* read-only SQL enforcement via sqlglot in sql() tool (Issue [#40](https://github.com/IceRhymers/spark-connect-mcp/issues/40)) ([16e866d](https://github.com/IceRhymers/spark-connect-mcp/commit/16e866d969e2697cf58f4c75a54a6b0a690b2d63))
* register dataframes tool module import in server ([0c6171d](https://github.com/IceRhymers/spark-connect-mcp/commit/0c6171df36fbdfd3bda153f61a39cde78cf18872))


### Bug Fixes

* address PR review — Confidence enum, DataFrame type, complex explain test, integration test ([a548d36](https://github.com/IceRhymers/spark-connect-mcp/commit/a548d36739f77424a10ee62611812218e1150008))
* delegate session.py set_preflight_threshold to preflight module ([b63ac8d](https://github.com/IceRhymers/spark-connect-mcp/commit/b63ac8dbb47c65c63343aa5fdd3b5d8eb5ed8867))
* import UTC directly to fix datetime.UTC AttributeError ([014a084](https://github.com/IceRhymers/spark-connect-mcp/commit/014a0844ef73daa9d2b81b320ede5003b1006e50))
* move tool import to top of test_preflight.py (ruff E402) ([ebf27b8](https://github.com/IceRhymers/spark-connect-mcp/commit/ebf27b8e2b1c21a616e1cf67d392c42cbddf4ec7))
* replace Optional[X] with X | None for ruff UP045 (CI lint) ([ac49b6c](https://github.com/IceRhymers/spark-connect-mcp/commit/ac49b6c690c06ca82bca9f72adaf81575f194f75))
* ruff --fix and format ([5c3a979](https://github.com/IceRhymers/spark-connect-mcp/commit/5c3a979050bb51d66792a247691e1f3957778531))
* ruff --fix and format ([bbf198c](https://github.com/IceRhymers/spark-connect-mcp/commit/bbf198c98a330905e4aee7cb035f28134176566e))
* ruff --fix and format ([9b26812](https://github.com/IceRhymers/spark-connect-mcp/commit/9b268128400930d57508a1d1e12449f14b1f7368))
* ruff --fix and format ([b35d6b6](https://github.com/IceRhymers/spark-connect-mcp/commit/b35d6b6f61973245972a518de2cbe21e455e3af9))
* ruff format integration test ([f767a51](https://github.com/IceRhymers/spark-connect-mcp/commit/f767a512241e578b91c9d9d002cb373d08304e77))
* ruff format integration test file ([a0a0b74](https://github.com/IceRhymers/spark-connect-mcp/commit/a0a0b74c131cff0d92d6ec425ddfb8c5399e5cc6))
* use Any instead of DataFrame type annotation to avoid new mypy import-not-found errors ([8779214](https://github.com/IceRhymers/spark-connect-mcp/commit/87792149bdc50c892fec94e58d31dc95e0409f15))
* use datetime.UTC alias (ruff UP017) ([87f3208](https://github.com/IceRhymers/spark-connect-mcp/commit/87f3208b1ee0e2bec3ceff356fd1a53c0b797937))
* use project connector stack in integration test (get_connector + session_mod.registry) ([f7726db](https://github.com/IceRhymers/spark-connect-mcp/commit/f7726db0a8b8aec8a42093db4ff2e876fdde30a7))
* use UTC alias in tests (ruff UP017) ([1df6f0c](https://github.com/IceRhymers/spark-connect-mcp/commit/1df6f0caaf334af498a24f3713725dc873990869))


### Documentation

* document sql tool read-only enforcement and SPARK_CONNECT_MCP_ALLOW_WRITE_SQL env var ([d7aa134](https://github.com/IceRhymers/spark-connect-mcp/commit/d7aa134b02b7a7f22afee845d8a582c96a4c97b7))
