// Apply LABEL node colors for CatMapper domains in both databases.
// Usage in Neo4j Browser:
//   :source cypher/set_label_colors.cypher

:param colorRows => [
  {CMName: "PROJECTILE_POINT_TYPE", color: "#e6194b"},
  {CMName: "PROJECTILE_POINT_CLUSTER", color: "#3cb44b"},
  {CMName: "PROJECTILE_POINT", color: "#ffe119"},
  {CMName: "CERAMIC_TYPE", color: "#0082c8"},
  {CMName: "CERAMIC_WARE", color: "#f58231"},
  {CMName: "CERAMIC", color: "#911eb4"},
  {CMName: "PHYTOLITH", color: "#46f0f0"},
  {CMName: "BOTANICAL", color: "#f032e6"},
  {CMName: "FAUNA", color: "#d2f53c"},
  {CMName: "SUBSPECIES", color: "#fabebe"},
  {CMName: "SPECIES", color: "#008080"},
  {CMName: "SUBGENUS", color: "#e6beff"},
  {CMName: "GENUS", color: "#aa6e28"},
  {CMName: "FAMILY", color: "#fffac8"},
  {CMName: "ORDER", color: "#800000"},
  {CMName: "CLASS", color: "#aaffc3"},
  {CMName: "PHYLUM", color: "#808000"},
  {CMName: "KINGDOM", color: "#ffd8b1"},
  {CMName: "BIOTA", color: "#000080"},
  {CMName: "FEATURE", color: "#808080"},
  {CMName: "SITE", color: "#7b4173"},
  {CMName: "ADM0", color: "#d62728"},
  {CMName: "ADM1", color: "#2ca02c"},
  {CMName: "ADM2", color: "#ff7f0e"},
  {CMName: "ADM3", color: "#1f77b4"},
  {CMName: "ADM4", color: "#a9a9a9"},
  {CMName: "ADMD", color: "#9467bd"},
  {CMName: "ADME", color: "#8c564b"},
  {CMName: "ADML", color: "#e377c2"},
  {CMName: "ADMX", color: "#7f7f7f"},
  {CMName: "REGION", color: "#bcbd22"},
  {CMName: "DISTRICT", color: "#17becf"},
  {CMName: "PERIOD", color: "#393b79"},
  {CMName: "DIALECT", color: "#637939"},
  {CMName: "LANGUAGE", color: "#8c6d31"},
  {CMName: "LANGUOID", color: "#843c39"},
  {CMName: "ETHNICITY", color: "#7b4173"},
  {CMName: "RELIGION", color: "#3182bd"},
  {CMName: "OCCUPATION", color: "#fdd0a2"},
  {CMName: "POLITY", color: "#a1d99b"},
  {CMName: "CULTURE", color: "#9e9ac8"},
  {CMName: "STONE", color: "#f768a1"},
  {CMName: "DATASET", color: "#41ab5d"},
  {CMName: "GENERIC", color: "#6baed6"},
  {CMName: "VARIABLE", color: "#d6616b"}
];

// ----------------------------
// ArchaMap
// ----------------------------
:use ArchaMap
UNWIND $colorRows AS row
MATCH (l:LABEL {CMName: row.CMName})
SET l.color = row.color
RETURN count(l) AS archaMapUpdated;

// Optional check: label CMNames in map that were not found in ArchaMap
UNWIND $colorRows AS row
OPTIONAL MATCH (l:LABEL {CMName: row.CMName})
WITH row, l
WHERE l IS NULL
RETURN collect(row.CMName) AS archaMapMissingLabels;

// ----------------------------
// SocioMap
// ----------------------------
:use SocioMap
UNWIND $colorRows AS row
MATCH (l:LABEL {CMName: row.CMName})
SET l.color = row.color
RETURN count(l) AS socioMapUpdated;

// Optional check: label CMNames in map that were not found in SocioMap
UNWIND $colorRows AS row
OPTIONAL MATCH (l:LABEL {CMName: row.CMName})
WITH row, l
WHERE l IS NULL
RETURN collect(row.CMName) AS socioMapMissingLabels;
