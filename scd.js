/**
 * Builds a type-2 slowly changing dimensions table and view.
 */
module.exports = (
    name,
    { 
      strategy,
      uniqueKey, 
      updatedAt, 
      checkCols,
      source, 
      tags, 
      columns = {},
      incrementalConfig,
      dependencies
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
      if (!updatedAt) throw new Error("Timestamp strategy requires 'updatedAt'");
      return `CAST(${aliasSrc}.${updatedAt} AS TIMESTAMP) > ${aliasT}.${updatedAt}`;
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
    // Timestamp strategy: use updatedAt as scd_valid_from
    return `
SELECT
  s.*,
  ${scdIdExpr},
  s.${updatedAt} AS scd_valid_from,
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
    // Timestamp strategy: simple LEAD over updatedAt
    return `
SELECT
  * EXCEPT( scd_valid_from, scd_valid_to ),
  ${updatedAt} AS scd_valid_from,
  LEAD(${updatedAt}) OVER (PARTITION BY ${isCompositeKey ? uniqueKey.join(", ") : uniqueKey} ORDER BY ${updatedAt} ASC) AS scd_valid_to
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
