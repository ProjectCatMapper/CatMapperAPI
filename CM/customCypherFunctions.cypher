
// Code to combine properties safely depending on type
CALL apoc.custom.declareFunction(
'formatProperties(prop::ANY, propType::STRING, sep::STRING) :: ANY',
'
WITH CASE
         WHEN apoc.meta.cypher.type($prop) CONTAINS "LIST" THEN $prop
         ELSE [$prop]
     END AS prop
WITH apoc.coll.toSet(apoc.coll.flatten([i in prop where not i is null],TRUE)) as prop
UNWIND prop as p
WITH split(p,$sep) as prop
UNWIND prop as p
WITH trim(p) as p
WITH
CASE
    WHEN toUpper($propType) = "INTEGER" THEN toString(toInteger(p))
    WHEN toUpper($propType) = "STRING" THEN toString(p)
    ELSE p
END AS p
WITH collect(distinct p) as prop
WITH [i in prop where not i = ""] as prop
WITH
CASE
    WHEN not toUpper($propType) = "LIST" THEN apoc.text.join(prop, "; ")
    ELSE prop
End as prop
WITH
CASE
    WHEN prop = [] or prop = "" THEN NULL
    ELSE prop
End as prop
RETURN prop

'
);

// get display name
CALL apoc.custom.declareFunction('getDisplayName(value :: STRING) :: STRING',
  'match (l:LABEL) where l.label = $value return l.displayName as name');

// create log
CALL apoc.custom.declareFunction(
  'makeLog(log::LIST OF STRING, user::STRING, msg::STRING) ::STRING',
  'return $log + (toString(datetime()) + " user " + $user + " " +  $msg)'
);


// Return single label
CALL apoc.custom.declareFunction(
  'getLabel(node::NODE) ::STRING',
  'with $node as c call {with c match (l:LABEL) where l.label in labels(c) and not l.label = "CATEGORY" with collect(l) as ls, count(*) as n unwind ls as l return l.label as label, l.groupLabel as group, n} call apoc.when(n > 1,"with label, group where not label = group return collect(label) as label","return label",{n:n, label:label, group:group}) yield value return apoc.text.join(apoc.coll.flatten(collect(value.label),true),":") as label'
);



// Delete empty lists in properties
CALL apoc.custom.declareProcedure(
  'deleteIfEmptyList(rel::RELATIONSHIP, propertyName::STRING) :: (relationship::RELATIONSHIP)',
  'WITH $rel as rel, $propertyName as propertyName WHERE isEmpty(rel[propertyName]) CALL apoc.cypher.doIt("with $rel as rel set rel." + $propertyName + " = NULL return rel",{rel: rel})  yield value RETURN value.rel as relationship', 'write'
);


// Return current year for null years
CALL apoc.custom.declareFunction('getYear(year::ANY) ::LIST OF INT',
'with case when not isEmpty(apoc.coll.flatten(collect($year),true)) then apoc.coll.flatten(collect($year),true) else [apoc.date.toYears(date(),"YYYY-MM-dd")] end as year RETURN toInteger(head(year))');

CALL apoc.custom.declareFunction(
'getNonNullProp(prop1::ANY, prop2::ANY, propType::STRING) :: ANY',
'
WITH toLower($propType) AS lowerPropType

// Separate logic for each type to ensure only the relevant block is evaluated
WITH lowerPropType,

// Integer logic block
CASE
    WHEN lowerPropType = "integer" THEN
        CASE
            WHEN apoc.meta.cypher.type($prop1) = "INTEGER" AND apoc.meta.cypher.type($prop2) = "INTEGER" THEN toInteger($prop1) + toInteger($prop2)
            WHEN apoc.meta.cypher.type($prop1) = "INTEGER" THEN toInteger($prop1)
            WHEN apoc.meta.cypher.type($prop2) = "INTEGER" THEN toInteger($prop2)
            ELSE NULL
        END

    // String logic block
    WHEN lowerPropType = "string" OR lowerPropType = "jsonmap" THEN
        CASE
            WHEN apoc.meta.cypher.type($prop1) = "STRING" AND apoc.meta.cypher.type($prop2) = "STRING" THEN
                CASE
                    WHEN trim($prop1) <> "" AND trim($prop2) = "" THEN trim($prop1)
                    WHEN trim($prop2) <> "" AND trim($prop1) = "" THEN trim($prop2)
                    WHEN trim($prop1) <> "" AND trim($prop2) <> "" THEN trim($prop1) + "; " + trim($prop2)
                    ELSE NULL
                END
            WHEN apoc.meta.cypher.type($prop1) = "STRING" THEN
                CASE
                    WHEN trim($prop1) <> "" THEN trim($prop1)
                    ELSE NULL
                END
            WHEN apoc.meta.cypher.type($prop2) = "STRING" THEN
                CASE
                    WHEN trim($prop2) <> "" THEN trim($prop2)
                    ELSE NULL
                END
            ELSE NULL
        END

    // List logic block (for cases not covered by integers or strings)
    ELSE
        CASE
            WHEN $prop1 IS NOT NULL AND $prop2 IS NOT NULL THEN apoc.coll.toSet(split(COALESCE($prop1, ""), ";") + split(COALESCE($prop2, ""), ";"))
            ELSE apoc.coll.toSet([x IN split(COALESCE($prop1, "") + ";" + COALESCE($prop2, ""), ";") WHERE trim(x) <> ""])
        END
END AS prop

WITH prop, lowerPropType
RETURN prop
'
);


// Get matching distance
CALL apoc.custom.declareFunction('matchingDist(Name::ANY, term::STRING) :: MAP',
'with custom.anytoList($Name) as Name, $term as term with Name, [i in Name | apoc.text.levenshteinDistance(custom.cleanText(term),custom.cleanText(i))] as score with Name, score, apoc.coll.min(score) as min with Name, score, min, apoc.coll.indexOf(score,min) as position return Name[position] as matching, min as score');

// Escape characters for matching
CALL apoc.custom.declareFunction('escapeText(text::STRING) :: STRING',
  'with replace($text,"-","\\-") as text ' +
  'with replace(text,"/","\\/") as text ' +
  'with replace(text,"(","\\(,") as text ' +
  'with replace(text,")","\\)") as text ' +
  'with replace(text,"!","\\!") as text ' +
  'with replace(text,"^","\\^") as text ' +
  'with replace(text,"[","\\[") as text ' +
  'with replace(text,"]","\\]") as text ' +
  'with replace(text,"+","\\]") as text ' +
  'return text');

// Clean text for matching
CALL apoc.custom.declareFunction('cleanText(text::STRING) :: STRING',
  'with replace($text,"\\\\"," ") as text ' +
  'with replace(text,"-"," ") as text ' +
  'with replace(text,"/"," ") as text ' +
  'with replace(text,"\\""," ") as text ' +
  'with replace(text,"("," ") as text ' +
  'with replace(text,")"," ") as text ' +
  'with replace(text,":"," ") as text ' +
  'with replace(text,"!"," ") as text ' +
  'with replace(text,"^"," ") as text ' +
  'with replace(text,"["," ") as text ' +
  'with replace(text,"]"," ") as text ' +
  'with replace(text,">"," ") as text ' +
  'with replace(text,"<"," ") as text ' +
  'with replace(text," ","\\ ") as text ' +
  'with tolower(text) as text ' +
  'return text');

// get a random list of nodes
CALL apoc.custom.declareFunction('randomNodes(no::INT, label::STRING) :: ANY',
  "MATCH (node) WHERE NOT isempty([i IN labels(node) WHERE i = $label]) WITH node, RAND() AS randomOrder ORDER BY randomOrder LIMIT $no RETURN id(node) AS nodeId");

// get a list of names deemed NOT credible
CALL apoc.custom.declareFunction('getCredibleNames(credible::ANY) :: ANY',
  "call apoc.case([$credible is not null,'unwind apoc.coll.flatten([$cred],true) as credible  with apoc.convert.fromJsonList(credible) as ls unwind ls as l with case when l.property = \"Name\" then l.value else NULL end as credible return custom.anytoList(collect(credible)) as result', $credible is null, 'return [] as result'],'RETURN [] as result', {cred:$credible}) yield value with value return value.result as result");

// get the name of a node from its CMID

CALL apoc.custom.declareFunction('getName(id :: ANY) :: STRING',
  'unwind $id as id MATCH (n:CATEGORY) where n.CMID = id RETURN apoc.text.join(collect(n.CMName),", ") as name');

// get the name of a node from its glottocode
CALL apoc.custom.declareFunction('getGlot(id :: ANY) :: STRING',
  'unwind $id as id MATCH (n:LANGUOID) where n.CMID = id RETURN apoc.text.join(collect(n.glottocode),", ") as name');


// convert any to list
call apoc.custom.declareFunction(
  'anytoList(val :: ANY, join = false :: BOOLEAN) :: STRING',
  'call apoc.when($join,"with apoc.text.join(apoc.coll.toSet(apoc.coll.flatten(collect($val1),true)),\', \') as output return output","with apoc.coll.toSet(apoc.coll.flatten(collect($val1),true)) as output return output",{val1:$val}) yield value return value.output as result');

// get any property from another property not working
call apoc.custom.declareFunction(
  'getProp(CMID::STRING val::STRING prop::string) :: STRING',
  'match (a) where a[$CMID] = $val return a[$prop]'
);
//CALL apoc.custom.asFunction('getProp',
//'match (a) where a[$CMID] = $val return a',
//'ANY',
//[['input','ANY'],['join','BOOL']],
//false,
// 'convert any input to list');

// get max year and accept any property type
call apoc.custom.declareFunction(
  'getMaxYear(val::ANY) :: STRING',
  'with toString(apoc.coll.max(apoc.coll.flatten(collect($val),true))) as result return result');

// get min year and accept any property type
call apoc.custom.declareFunction(
  'getMinYear(val::ANY) :: STRING',
  'with toString(apoc.coll.min(apoc.coll.flatten(collect($val),true))) as result return result');
