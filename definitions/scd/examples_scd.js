const scd = require("../../scd");


//Check strategy comparing specific columns
scd("src_check", {
  strategy: "check",
  uniqueKey: ["user_id", "client_id"],
  checkCols: ["value"],
  source: { 
    schema: dataform.projectConfig.defaultDataset, 
    name: "src_strategy_check" 
  },
  tags: ['scd', 'full'],
  dependencies: ['update_src_check']
});


//Check strategy comparing specific columns
scd("src_timestamp", {
  strategy: "timestamp",
  uniqueKey: ["user_id", "client_id"],
  updatedAt: 'updated_at',
  source: { 
    schema: dataform.projectConfig.defaultDataset, 
    name: "src_strategy_timestamp" 
  },
  tags: ['scd', 'full'],
  dependencies: ['update_src_timestamp']
});

