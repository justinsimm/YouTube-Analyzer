// NOTE: The following script syntax is valid for database version 5.0 and above.

:param {
  // Define the file path root and the individual file names required for loading.
  // https://neo4j.com/docs/operations-manual/current/configuration/file-locations/
  file_path_root: 'file:///', // Change this to the folder your script can access the files at.
  file_0: 'videos.csv',
  file_1: 'related.csv'
};

// CONSTRAINT creation
// -------------------
//
// Create node uniqueness constraints, ensuring no duplicates for the given node label and ID property exist in the database. This also ensures no duplicates are introduced in future.
//
CREATE CONSTRAINT `videoId_Videos_uniq` IF NOT EXISTS
FOR (n: `Videos`)
REQUIRE (n.`videoId`) IS UNIQUE;
CREATE CONSTRAINT `videoId_Videos_uniq` IF NOT EXISTS
FOR (n: `Videos`)
REQUIRE (n.`videoId`) IS UNIQUE;

:param {
  idsToSkip: []
};

// NODE load
// ---------
//
// Load nodes in batches, one node label at a time. Nodes will be created using a MERGE statement to ensure a node with the same label and ID property remains unique. Pre-existing nodes found by a MERGE statement will have their other properties set to the latest values encountered in a load file.
//
// NOTE: Any nodes with IDs in the 'idsToSkip' list parameter will not be loaded.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`videoId` IN $idsToSkip AND NOT row.`videoId` IS NULL
CALL (row) {
  MERGE (n: `Videos` { `videoId`: row.`videoId` })
  SET n.`videoId` = row.`videoId`
  SET n.`uploader` = row.`uploader`
  SET n.`categoryId` = toInteger(trim(row.`categoryId`))
  SET n.`category` = row.`category`
  SET n.`metric1` = toInteger(trim(row.`metric1`))
  SET n.`views` = toInteger(trim(row.`views`))
  SET n.`rating` = toFloat(trim(row.`rating`))
  SET n.`ratingCount` = toInteger(trim(row.`ratingCount`))
  SET n.`commentCount` = toInteger(trim(row.`commentCount`))
} IN TRANSACTIONS OF 10000 ROWS;

LOAD CSV WITH HEADERS FROM ($file_path_root + $file_0) AS row
WITH row
WHERE NOT row.`videoId` IN $idsToSkip AND NOT row.`videoId` IS NULL
CALL (row) {
  MERGE (n: `Videos` { `videoId`: row.`videoId` })
  SET n.`videoId` = row.`videoId`
  SET n.`uploader` = row.`uploader`
  SET n.`categoryId` = toInteger(trim(row.`categoryId`))
  SET n.`category` = row.`category`
  SET n.`metric1` = toInteger(trim(row.`metric1`))
  SET n.`views` = toInteger(trim(row.`views`))
  SET n.`rating` = toFloat(trim(row.`rating`))
  SET n.`ratingCount` = toInteger(trim(row.`ratingCount`))
  SET n.`commentCount` = toInteger(trim(row.`commentCount`))
} IN TRANSACTIONS OF 10000 ROWS;


// RELATIONSHIP load
// -----------------
//
// Load relationships in batches, one relationship type at a time. Relationships are created using a MERGE statement, meaning only one relationship of a given type will ever be created between a pair of nodes.
LOAD CSV WITH HEADERS FROM ($file_path_root + $file_1) AS row
WITH row 
CALL (row) {
  MATCH (source: `Videos` { `videoId`: row.`srcVideoId` })
  MATCH (target: `Videos` { `videoId`: row.`dstVideoId` })
  MERGE (source)-[r: `Related_Videos`]->(target)
  SET r.`srcVideoId` = row.`srcVideoId`
  SET r.`dstVideoId` = row.`dstVideoId`
} IN TRANSACTIONS OF 10000 ROWS;
