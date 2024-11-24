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

## Setup

    ## Build the db schema
    cd webserver
    mvn clean install -DskipTests
    mvn spring-boot:run
    ## If you dont need the webserver, shut it down

    cd indexer
    cp prise.example.properties prise.properties
    ## Edit properties as needed; particularly database (x2) url + login, cardano-node (cnode) url and API keys for any data API used

    mkdir /var/log/prise/
    chown -R <user>:<user> /var/log/prise

## Build

    cd indexer
    mvn clean install -DskipTests

## Run

    mvn exec:exec -Dconfig=prise.properties

## Configs
####    prise.properties - app configuration

    run.mode=livesync|oneshot
    latest.prices.livesync.update.interval.seconds=
    make.historical.data=true|false
    start.metrics.server=true|false
    metrics.server.port=
    app.datasource.driver-class-name=
    app.datasource.url=
    app.datasource.username=
    app.datasource.password=
    token.metadata.service.module=tokenRegistry
    chain.database.service.module=koios|carpJDBC
    cnode.address=
    cnode.port=
    start.point.time=

     *see prise.example.properties for more detailed descriptions

####    src/main/resources/environment.properties

    # selects specific logback config logack-$logEnv.xml
    logEnv=prod|dev

## Components
| Component | Description                                                                                                       | 
|-----------|-------------------------------------------------------------------------------------------------------------------|
| indexer   | Core component which listens to blockchain, parses trade data, computes and persists latest and historical prices |  
| webserver | Optional; simple Spring Boot webserver with latest price and historical price endpoints                           |   

## Dependencies
| Dependency                                                                             | Description                                                                                                                                                                                                                                                                          | 
|----------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [Cardano-node](https://github.com/IntersectMBO/cardano-node)                           | Access to a fully synced Cardano node instance, for best performance in terms of stability and sync speed, use a dedicated node on a local network. Tested with cardano-node v8.9.2                                                                                                  |
| [Carp](https://github.com/dcSpark/carp) (Only if using Carp module)                    | Access to a fully synced Carp database. Carp is a general purpose modular Cardano indexer using Posgresql. This is required primarily to resolve utxo references. Other alternatives will work also however will require custom implementation of the ChainDatabaseService interface |
| [Yaci Store](https://github.com/bloxbean/yaci-store) (Only if using Yaci Store module) | Access to a fully synced Yaci Store database. Yaci Store is a general purpose modular Cardano indexer using Posgresql or MySQL. This is required primarily to resolve utxo references. Other alternatives will work also however will require custom implementation of the ChainDatabaseService interface 
## Modules
| Module               | Description                                                                                                                                                  | Implementations                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | 
|----------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ChainDatabaseService | Implementations provide an ability to resolve transaction output references (e.g. txHash#index) and searching for the nearest block to a given slot/time | <li>**Blockfrost (preferred)** Requires *blockfrost.datasource* properties configured in the prise.properties config file as well as a project token you can register for at <a href="https://blockfrost.io">blockfrost.io</a> with ability to scale up needs</li><li>**Yaci-Store** Requires *yacistore.datasource* properties configured in the prise.properties config file as well as network access to a fully synced Yaci Store webservice</li><li>**CarpJdbcService** Requires *carp.datasource* properties configured in the prise.properties config file as well as network access to a fully synced Carp (Postgres) database </li><li title="Work in progress">**KoiosService (WIP)**</li>[Koios](https://www.koios.rest) is a decentralised Api that can provide some of the required data. Currently it can only perform the transaction output resolutions thus you will need an alternate method to find nearest block to slot (external api or otherwise). Performance is limited, more suitable for development testing <li>**CarpHttpService (WIP)**</li>Preferred over CarpJdbcService for simplicity, however the default webserver doesn't provide all necessary endpoints. Currently it can resolve transaction output references only and would need a custom webserver to implement the getBlockNearestToSlot function |  
| TokenMetadataService | Implementations provide the ability to resolve token decimals specifications for Cardano Native Tokens                                             | <li>TokenMetadataService</li><br/>The tokens.cardano.org service<br/>Other implementations can be added as required                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |

## Java Version

    Tested with Java 11
    * Have noticed at least one issue using Java 20
      due to an issue with the KoinTest Mocking framework 
      (Unsupported class file major version 64)

## Mysql Installation

[Official Linux Installation Guide](https://dev.mysql.com/doc/refman/8.0/en/linux-installation.html)<br/>
[Official macOS Installation Guide](https://dev.mysql.com/doc/refman/8.0/en/macos-installation.html)

### Add a mysql user:
    Login as mysql root user
    mysql> CREATE USER 'prise'@'localhost' IDENTIFIED BY '<password>';
    mysql> GRANT ALL PRIVILEGES ON prise.* TO 'prise'@'localhost' WITH GRANT OPTION;
    mysql> flush privileges;

### Miscellaneous

    * Make sure Operating System is using a timezone without daylight savings. Otherwise will see odd weekly candles misaligned
    e.g. timedatectl set-timezone Asia/Kuala_Lumpur

## Contributions
Contributions welcome

## Support
This project is made possible by Delegators to the [AUSST](https://ausstaker.com.au) Cardano Stakepool and 
supporters of [Edgx](https://edgx.tech) R&D