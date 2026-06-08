# Testing

Two tiers across both modules (`indexer`, `webserver`):

- **Unit tests** — fast, hermetic, no network or database. Run on every build.
- **Integration tests** — tagged `@Tag("integration")`, hit live external
  services / a running app. Excluded from `test`; run on demand.

## Unit tests

```sh
./gradlew test            # both modules
./gradlew :indexer:test
./gradlew :webserver:test
```

Run out of the box on a fresh clone — no setup. `ConfigTest` reads committed,
secret-free templates (`indexer/src/test/resources/*.example.properties`).

## Integration tests

```sh
./gradlew integrationTest             # both modules
./gradlew :indexer:integrationTest
./gradlew :webserver:integrationTest
```

### indexer

Self-contained — each test creates its own HTTP client against an external
provider. No locally-running app needed; some need a Postgres DB, most don't.

| Test | Hits | Needs |
| --- | --- | --- |
| `TokenRegistryServiceIT` | `tokens.cardano.org` (public) | nothing |
| `BlockfrostServiceIT` | Blockfrost API | valid `blockfrost.datasource.apikey` |
| `KoiosServiceIT` | Koios **+ Blockfrost** | Koios URL **and** a valid Blockfrost key |
| `YaciStoreServiceOT` | your yaci-store deployment | `yacistore.datasource.url` + token |

**Setup** — supply config in `indexer/src/test/resources/prise.properties`
(gitignored, so your keys stay out of git):

```sh
cd indexer/src/test/resources
cp prise.example.properties prise.properties
# set chain.database.service.module + fill in that provider's URL / API key.
# Keys may also come from env vars (BLOCKFROST_DATASOURCE_APIKEY,
# KOIOS_DATASOURCE_APIKEY, YACISTORE_DATASOURCE_URL, ...) which override the file.
```

**Fail-fast:** in production these services retry a down endpoint many times
(YaciStore ≈ 27 min). The `integrationTest` task sets
`-Dprise.maxProviderAttempts=2` so a down/misconfigured endpoint fails in
seconds. Override for production-like retries:
`./gradlew :indexer:integrationTest -Dprise.maxProviderAttempts=100`.

### webserver

`ApiControllerIT` is **black-box**: it sends real HTTP requests to a **running**
webserver at `http://localhost:8092` and asserts on the responses — it does not
start the app itself.

| Dependency | Default | Why |
| --- | --- | --- |
| Webserver | `http://localhost:8092` | the target under test |
| Postgres | `localhost:5432/prise` | price/candle data (JPA datasource) |
| Redis | `localhost:6379` | caching |

```sh
# terminal 1 — start Postgres + Redis, then the app:
./gradlew :webserver:bootRun        # serves on :8092
# terminal 2 — once it is listening:
./gradlew :webserver:integrationTest
```

`java.net.ConnectException` for every test means the webserver isn't up — not a
test failure. The task fails fast (no retries).
