const scdBuilder = require("../../index");

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
