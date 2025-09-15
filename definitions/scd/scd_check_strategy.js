const scdBuilder = require("../../index");

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
