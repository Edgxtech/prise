<div style="text-align: center;">

[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Edgxtech/prise/blob/master/LICENSE)
</div>

# Prise - Cardano Native Token Price Indexer

Kotlin based indexer as used by: [https://realfi.info](https://realfi.info). Provides historical and latest prices for all Cardano Native Tokens (CNT). Uses;
-   Bloxbean [Yaci](https://github.com/bloxbean/yaci) JVM based Cardano mini-protocols library
-   Blockbean [YaciStore](https://github.com/bloxbean/yaci-store) general purpose indexer, only for txOutput resolution (modular option)
-   dcSpark [Carp](https://github.com/dcSpark/carp) general purpose indexer, only for txOutput resolution (modular option)
-   Koios [Koios](https://api.koios.rest) Cardano data API, only for txOutput resolution (modular option)
-   Blockfrost [Blockfrost](https://blockfrost.io) Cardano data API, only for txOutput resolution (modular option)
-   Cardano Foundation [Token Registry](https://github.com/cardano-foundation/cardano-token-registry) for CNT metadata (modular, can be swapped)
-   Trades obtained from on-chain Cardano DEX data including Minswap V1/V2, Sundaeswap V1 and Wingriders (modular can be extended)
-   Koin dependency injection

## Quickstart

**Configure your environment**

```bash
cp .env.template .env
```

Modify `.env`, in particular, these are required. You may modify other variables if needed
```properties
BLOCKFROST_DATASOURCE_APIKEY=<yours, get from https://blockfrost.io>
DATABASE_USERNAME=<yours>
DATABASE_PASSWORD=<yours>
```

**Run the system**

From the project root,
```bash
docker compose up -d
```

```bash
scripts/check_running.sh
```

Wait until a state similar to the following
```
=== Docker Compose Stats Snapshot ===
CONTAINER ID   NAME               CPU %     MEM USAGE / LIMIT     MEM %     NET I/O           
32c84ac16774   prise-app-1        130.91%   328.4MiB / 9.704GiB   3.30%     16.7kB / 16.1kB   
f0621088a4e9   prise-indexer-1    0.51%     215MiB / 9.704GiB     2.16%     24.6kB / 21kB     
5d5b53bee447   prise-redis-1      0.09%     19.23MiB / 9.704GiB   0.19%     2.26kB / 126B    
acaad51cd1aa   prise-postgres-1   0.07%     89.8MiB / 9.704GiB    0.90%     35.9kB / 31.3kB   

=== Health Checks ===
Checking Postgres...
localhost:5433 - accepting connections
✓ Postgres is ready
Checking Indexer (http://localhost:9108/metrics) ...
✓ Indexer is responding (HTTP 200)
Checking Prise App (http://localhost:8092/tokens/symbols) ...
✓ Prise App is responding (HTTP 200)
Checking Redis...
✓ Redis is ready
All checks passed successfully!
```

Now you will have API endpoints available for price data, e.g. 
```bash
curl localhost:8092/prices/latest
{"date":"2025-08-14T04:10:38","assets":[{...}]}
```

```bash
curl localhost:8092/tokens/pairs        
[{"first":"25c5de5f5b286073c593edfd77b48abc7a48e5a4f3d4cd9d428ff93557414e","second":"ADA"},...}]
```

The indexer will take some time to process the chain data though, you can check progress in the logs like so
```bash
docker compose logs indexer -f
```

Which will show progress similar to the following
```
indexer | INFO ChainService : Processed Block >> 12285130, 8cbb9cbcd8678ace348c9ad77b1fa82d26d8fcbb26419790251d82778bf62251, 164186491
```

And you can view API documentation locally at: http://localhost:8092/swagger-ui/index.html


## Build & Run with Docker

```bash
cd indexer
./gradlew clean build -x test
```

Configure `.env`, we don't need to export since docker-compose handles this
```bash
cp .env.template .env
## Modify .env, in particular, database, cardano-node and API Keys
```

Build and run in docker
```bash
docker build -t edgxtech/prise-indexer .
docker compose up indexer -d
docker compose logs -f
```

## Build & Run for Development

### PostgreSQL Installation

[Official Installation Guide](https://www.postgresql.org/docs/current/installation.html)

### Add a postgres user:

    Login as postgres root user
    postgres=# CREATE DATABASE prise;
    postgres=# CREATE USER prise WITH PASSWORD '<yours>';
    postgres=# GRANT USAGE, CREATE ON SCHEMA public TO prise;
    postgres=# GRANT ALL PRIVILEGES ON DATABASE prise TO prise;

### Build the db schema

```bash
cd indexer
./gradlew flywayMigrate
```

### Set Java

```bash
## It is necessary to have JAVA_HOME env variable set for the indexer to run, othewise you may see a 'Java Directory not found' type of error
## On Linux Java home is likely in /usr/lib/jvm/, On MacOS likely in ~/Library/Java/JavaVirtualMachines/
## Also suggest adding this export to your terminal profile, e.g. one of; ~/.bashrc, ~/zprofile, ~/.bash_profile
export JAVA_HOME=<Your Java Install>
```

### Build jar
```bash
cd indexer
./gradlew clean build -x test
```

To configure, either **modify .env and export all environment variables**
```bash
cp .env.template .env
## Modify .env, in particular, database, cardano-node and API Keys
set -a
source .env
set +a
```

Or **customise the prise.properties file**

then run the indexer with

```bash
java -jar build/libs/indexer-0.1.0.jar -config prise.properties
```

## Configs
####    prise.properties - app configuration

    run.mode=livesync|oneshot
    latest.prices.livesync.update.interval.seconds=
    start.metrics.server=true|false
    metrics.server.port=
    app.datasource.driver-class-name=
    app.datasource.url=
    app.datasource.username=
    app.datasource.password=
    token.metadata.service.module=tokenRegistry
    chain.database.service.module=blockfrost|yacistore
    cnode.address=
    cnode.port=
    start.point.time=
    # selects specific logback config logack-$logEnv.xml
    log.env=default|prod|dev

     *see prise.properties for more detailed descriptions


## Components
| Component | Description                                                                                                       | 
|-----------|-------------------------------------------------------------------------------------------------------------------|
| indexer   | Core component which listens to blockchain, parses trade data, computes and persists latest and historical prices |  
| webserver | Optional; simple Spring Boot webserver with latest price and historical price endpoints                           |   

## Dependencies
| Dependency                                                                             | Description                                                                                                                                                                                                                                                                                                | 
|----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Cardano-node](https://github.com/IntersectMBO/cardano-node)                           | Access to a fully synced Cardano node instance, for best performance in terms of stability and sync speed, use a dedicated node on a local network. Tested with cardano-node v10.1.4                                                                                                                       |
| [Carp](https://github.com/dcSpark/carp) (Only if using Carp module)                    | Access to a fully synced Carp database. Carp is a general purpose modular Cardano indexer using Posgresql. This is required primarily to resolve utxo references. Other alternatives will work also, however will require custom implementation of the ChainDatabaseService interface                      |
| [Yaci Store](https://github.com/bloxbean/yaci-store) (Only if using Yaci Store module) | Access to a fully synced Yaci Store database. Yaci Store is a general purpose modular Cardano indexer using Posgresql or MySQL. This is required primarily to resolve utxo references. Other alternatives will work also, however will require custom implementation of the ChainDatabaseService interface 

## Modules
| Module               | Description                                                                                                                                                  | Implementations                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | 
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ChainDatabaseService | Implementations provide an ability to resolve transaction output references (e.g. txHash#index) and searching for the nearest block to a given slot/time | <li>**Blockfrost (preferred)** Requires *blockfrost.datasource* properties configured in the prise.properties config file as well as a project token you can register for at <a href="https://blockfrost.io">blockfrost.io</a> with ability to scale up needs</li><li>**Yaci-Store** Requires *yacistore.datasource* properties configured in the prise.properties config file as well as network access to a fully synced Yaci Store webservice</li><li>**CarpJdbcService** Requires *carp.datasource* properties configured in the prise.properties config file as well as network access to a fully synced Carp (Postgres) database </li><li title="Work in progress">**KoiosService (WIP)**</li>[Koios](https://www.koios.rest) is a decentralised Api that can provide some of the required data. Currently it can only perform the transaction output resolutions thus you will need an alternate method to find nearest block to slot (external api or otherwise). Performance is limited, more suitable for development testing <li>**CarpHttpService (WIP)**</li>Preferred over CarpJdbcService for simplicity, however the default webserver doesn't provide all necessary endpoints. Currently it can resolve transaction output references only and would need a custom webserver to implement the getBlockNearestToSlot function |  
| TokenMetadataService | Implementations provide the ability to resolve token decimals specifications for Cardano Native Tokens                                             | <li>**TokenMetadataService**</li>The tokens.cardano.org service<br/>Other implementations can be added as required                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

## Java Version

    Tested with Java 11
    * Have noticed at least one issue using Java 20
      due to an issue with the KoinTest Mocking framework 
      (Unsupported class file major version 64)

### Miscellaneous

    * Make sure Operating System is using a timezone without daylight savings. Otherwise will see odd weekly candles misaligned
    e.g. timedatectl set-timezone Asia/Kuala_Lumpur

## Contributions
Contributions welcome

## Support
This project is made possible by Delegators to the [AUSST](https://ausstaker.com.au) Cardano Stakepool and 
supporters of [Edgx](https://edgx.tech) R&D