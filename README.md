# SCD Builder for Dataform


Dataform Core is an open source meta-language to create SQL tables and workflows in BigQuery. 
It is the best alternative to DBT.

This package provides a reusable JavaScript helper for building **Type-2 Slowly Changing Dimension (SCD)** tables and views in [Dataform](https://dataform.co).  
It automates the creation of an incremental *historical table* and a user-friendly *SCD view*.

---

## Overview

When you call the builder, it generates:

__1.__  
```yaml
INCREMENTAL TABLE  →  **<name>_historical**

        - Stores all historical versions of each row.  
        - Includes SCD metadata columns:  
            - scd_id: hash of primary key(s)  
            - scd_valid_from: timestamp when the record became valid  
            - scd_valid_to: timestamp when the record expired (NULL if current)  
```

__2.__ 
```yaml
VIEW  →  **<name>_scd**

        - Wraps the historical table for convenient querying.  
        - Computes `scd_valid_to` dynamically.  
        - Supports both **timestamp** and **check** change detection strategies.
```

---

## Installation

Place the following into __package.json__

```json
{
    "name": "your-repository",
    "dependencies": {
        "@dataform/core": "3.0.26",
        "df-scd-builder": "https://github.com/tuliodalcol/scd-builder/archive/refs/tags/v1.0.0.tar.gz"
    }
}
```

## Example

Build Slowly-Changin-Dimension with strategy based on Primary or Composite Keys
```js
    const scdBuilder = require("df-scd-builder");

    //Check strategy comparing specific columns
    scdBuilder("src_check", {
        strategy: "check",
        uniqueKey: ["user_id", "client_id"],
        checkCols: ["value"],
        source: { 
            schema: dataform.projectConfig.defaultDataset, 
            name: "src_strategy_check" 
        },
        tags: ['scd', 'full'],
        dependencies: ['update_src_check'],
        schema: dataform.projectConfig.defaultDataset, 
        description: "Updates table for SCD with Check columns strategy",
    });
```

Build Slowly-Changin-Dimension with strategy based on Timestamp Column
```js
    const scdBuilder = require("df-scd-builder");

    //Timestamp strategy comparing keys related to timestamp column
    scdBuilder("src_timestamp", {
        strategy: "timestamp",
        uniqueKey: ["user_id", "client_id"],
        timestampCol: 'updated_at',
        source: { 
            schema: dataform.projectConfig.defaultDataset, 
            name: "src_strategy_timestamp" 
        },
        tags: ['scd', 'full'],
        dependencies: ['update_src_timestamp'],
        schema: dataform.projectConfig.defaultDataset, 
        description: "Updates table for SCD with Timestamp strategy",
    });
```



