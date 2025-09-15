/**
 * Builds a Type-2 Slowly Changing Dimension (SCD) historical table and view.
 *
 * This helper generates two published Dataform actions:
 *
 *  1. `<name>_historical` (incremental table)  
 *     - Stores all historical versions of each row based on the chosen SCD strategy.
 *     - Maintains SCD metadata columns (`scd_id`, `scd_valid_from`, `scd_valid_to`, `scd_active`).
 *
 *  2. `<name>_scd` (view)  
 *     - Exposes the historical table in a user-friendly SCD format.
 *     - Computes validity ranges (`scd_valid_from` → `scd_valid_to`) for each record.
 *
 * ---
 *
 * @function
 * @param {string} name - Base name for the SCD dataset.  
 *   - Creates `<name>_historical` (incremental table) and `<name>_scd` (view).
 *
 * @param {Object} config - Configuration options.
 * @param {"timestamp"|"check"} config.strategy - Strategy for detecting changes:
 *   - `"timestamp"`: Detects changes when `timestampCol` column is more recent than stored rows.
 *   - `"check"`: Detects changes when one or more specified `checkCols` differ.
 *
 * @param {string|string[]} config.uniqueKey - Primary key column(s) uniquely identifying rows.  
 *   - Can be a single column (string) or composite key (array of column names).
 *
 * @param {string} [config.timestampCol] - Column name for the "last updated" timestamp.  
 *   - Required when `strategy = "timestamp"`.  
 *   - Ignored when `strategy = "check"`, unless you include it in `checkCols`.
 *
 * @param {string[]} [config.checkCols] - List of columns to compare for equality.  
 *   - Required when `strategy = "check"`.  
 *   - If any differ, a new version is inserted.
 *
 * @param {string|{schema: string, name: string}} config.source - Source dataset/table reference.
 *   - Passed directly into `ctx.ref(source)`.
 *
 * @param {string[]} [config.tags] - Tags to assign to both published actions.
 *
 * @param {Object} [config.columns] - Column descriptions for documentation in Dataform.
 *
 * @param {Object} [config.incrementalConfig] - Additional configuration passed into the historical table publish call.  
 *   - E.g., `bigquery: { partitionBy: "DATE(scd_valid_from)" }`.
 *
 * @param {string[]} [config.dependencies] - Optional dependency action names for the historical table.
 *
 * ---
 *
 * @returns {{ histData: Action, scdView: Action }}
 * - `histData`: The incremental table action (`<name>_historical`).
 * - `scdView`: The view action (`<name>_scd`).
 *
 * ---
 *
 * @example
 * // Build an SCD using timestamp strategy
 * const { histData, scdView } = require("scd_builder")("user_snapshot", {
 *   strategy: "timestamp",
 *   uniqueKey: "user_id",
 *   timestampCol: "updated_at",
 *   source: "raw.users",
 *   tags: ["scd", "users"],
 *   incrementalConfig: {
 *     bigquery: { partitionBy: "DATE(scd_valid_from)" }
 *   }
 * });
 *
 * @example
 * // Build an SCD using check strategy with composite key
 * const { histData, scdView } = require("scd_builder")("client_orders", {
 *   strategy: "check",
 *   uniqueKey: ["client_id", "order_id"],
 *   checkCols: ["status", "amount"],
 *   source: "raw.orders",
 *   tags: ["scd", "orders"]
 * });
 */

module.exports = (
    name,
    { 
      strategy,
      uniqueKey, 
      timestampCol, 
      checkCols,
      source, 
      tags, 
      columns = {},
      incrementalConfig,
      dependencies,
      schema,
      description
    }
) => {
  if (!["check", "timestamp"].includes(strategy)) {
    throw new Error(`Invalid strategy: ${strategy}. Must be "check" or "timestamp".`);
  }

  // Bool if is CompositeKey
  const isCompositeKey = Array.isArray(uniqueKey);
  // Expression to generate hash scd_id
  const scdIdExpr = isCompositeKey
    ? `TO_HEX(MD5(CONCAT(${uniqueKey.map(k => `CAST(s.${k} AS STRING)`).join(", ")}))) AS scd_id`
    : `TO_HEX(MD5(CAST(${uniqueKey} AS STRING))) AS scd_id`;


  // Comparison expression generator
  const compareExpr = (aliasSrc = "s", aliasT = "t") => {
    if (strategy === "timestamp") {
      if (!timestampCol) throw new Error("Timestamp strategy requires 'timestampCol'");
      return `CAST(${aliasSrc}.${timestampCol} AS TIMESTAMP) > ${aliasT}.${timestampCol}`;
    }
    if (strategy === "check") {
      if (!checkCols.length) throw new Error("Check strategy requires 'checkCols'");
      return checkCols.map(c => `${aliasSrc}.${c} != ${aliasT}.${c}`).join(" OR ");
    }
  };

  // Incremental query generator
  const incrQry = (ctx) => `
SELECT 
  s.*,
  ${scdIdExpr},
  CURRENT_TIMESTAMP() AS scd_valid_from,
  CAST(NULL AS TIMESTAMP) AS scd_valid_to
FROM ${ctx.ref(source)} s
LEFT JOIN ${ctx.self()} t
  ON ${isCompositeKey
    ? uniqueKey.map(k => `s.${k} = t.${k}`).join(" AND ")
    : `s.${uniqueKey} = t.${uniqueKey}`}
WHERE t.${isCompositeKey ? uniqueKey[0] : uniqueKey} IS NULL
  OR (${compareExpr("s", "t")})
  `;

const fullRefreshQry = (ctx) => {
  if (strategy === "timestamp") {
    // Timestamp strategy: use timestampCol as scd_valid_from
    return `
SELECT
  s.*,
  ${scdIdExpr},
  s.${timestampCol} AS scd_valid_from,
  CAST(NULL AS TIMESTAMP) AS scd_valid_to
FROM ${ctx.ref(source)} s
    `;
  } else if (strategy === "check") {
    return `
SELECT
  s.*,
  ${scdIdExpr},
  CURRENT_TIMESTAMP() AS scd_valid_from,
  CAST(NULL AS TIMESTAMP) AS scd_valid_to
FROM ${ctx.ref(source)} s
    `;
  }
};


const view = (ctx) => {
  if (strategy === "timestamp") {
    // Timestamp strategy: simple LEAD over timestampCol
    return `
SELECT
  * EXCEPT( scd_valid_from, scd_valid_to ),
  ${timestampCol} AS scd_valid_from,
  LEAD(${timestampCol}) OVER (PARTITION BY ${isCompositeKey ? uniqueKey.join(", ") : uniqueKey} ORDER BY ${timestampCol} ASC) AS scd_valid_to
FROM ${ctx.ref(histData.proto.target.schema, `${name}_historical`)}
    `;
  } 
  
  else if (strategy === "check") {
    // Check strategy: detect changes based on checkCols
    const changeColsHash = `TO_HEX(MD5(CONCAT(${checkCols.map(c => `CAST(${c} AS STRING)`).join(", ")})))`;

    return `
SELECT
  * EXCEPT( scd_valid_to ),
  LEAD(scd_valid_from) OVER (PARTITION BY ${isCompositeKey ? uniqueKey.join(", ") : uniqueKey} ORDER BY scd_valid_from ASC) AS scd_valid_to
FROM ${ctx.ref(histData.proto.target.schema, `${name}_historical`)} t
ORDER BY ${isCompositeKey ? uniqueKey.join(", ") : uniqueKey}, scd_valid_from
    `;
  }
};


 const histData = publish(`${name}_historical`, {
    type: "incremental",
    dependencies: dependencies,
    schema: schema,
    description: description,
    //uniqueKey: uniqueKey,
    columns: {
      ...columns,
      scd_valid_from: "Timestamp from which this row is valid",
      scd_valid_to: "Timestamp until which this row is valid, or NULL if latest",
      scd_active: "1 if row is current, 0 otherwise",
      scd_id: "Generated hash based on primary key(s)"
    },
    tags: tags,
    ...incrementalConfig
  }).query(ctx => `
    ${ctx.when(ctx.incremental(), incrQry(ctx))}
    ${ctx.when(!ctx.incremental(), fullRefreshQry(ctx))}
  `);

 const scdView = publish(`${name}_scd`, {
    type: "view",
    schema: schema,
    description: description,
    tags: tags,
    columns: {
      ...columns,
      scd_valid_from: "Timestamp from which this row is valid",
      scd_valid_to: "Timestamp until which this row is valid, or NULL if latest",
      scd_active: "1 if row is current, 0 otherwise",
      scd_id: "Generated hash based on primary key(s)"
    },
  }).query(ctx => `
    ${view(ctx)}
  `);

  return { histData, scdView };
};
