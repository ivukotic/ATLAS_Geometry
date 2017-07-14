
// Get all the children nodes  of the RootVolume, and sort them by the 'position' property of the CHILD relationship
match (n:RootVolume)-[r:CHILD]->(m) with r,m order by r.position return r.position, m;

// Same as above, but returns the node labels as well
match (n:RootVolume)-[r:CHILD]->(m) with r,m order by r.position return r.position, m, labels(m);

// same as above, but just get the latest three children (position > 45)
match (n:RootVolume)-[r:CHILD]->(m) where r.position > 45 with r,m order by r.position return r.position, m, labels(m);

// same as above, but return the relationship as well: useful for the network visualization, so we can see the arrows between the selected nodes
match (n:RootVolume)-[r:CHILD]->(m) where r.position > 45 with r,m order by r.position return r, r.position, m, labels(m);

// return the NameTag node whose id is "45916" (use the quotes, because it is a string)
match (n:NameTag {id: "45916"}) return n;

// return all NameTag nodes which are children of the RootVolume, and order them by position; also, return the relationships.
match (n:RootVolume)-[r:CHILD]->(m:NameTag) with r,m order by r.position return r.position, m, labels(m), r;

// the same, but filter on "name" property of NameTag node
match (n:RootVolume)-[r:CHILD]->(m:NameTag) where m.name = "Tile" with r,m order by r.position return r.position, m, labels(m), r;

// get the LogVol node which is linked through a PhysVol node through a LOGVOL relationship and whose "name" property is "TileEndcapNeg"
match (n:PhysVol)-[r:LOGVOL]->(m:LogVol) where m.name = "TileEndcapNeg" return m, labels(m);

// the same, but returns the relationship as well, so you get the PhysVol node in the graph view
match (n:PhysVol)-[r:LOGVOL]->(m:LogVol) where m.name = "TileEndcapNeg" return m, labels(m), r;

// as above, but returns the RootVolume and the child relationship as well, i.e.:
// return the LogVol who is related to a PhysVol through a LOGVOL relationship and whith PhysVol related to RootVolume through a CHILD rel; also, return the "position" of the CHILD rel
match (a:RootVolume)-[r1:CHILD]->(n:PhysVol)-[r:LOGVOL]->(m:LogVol) where m.name = "TileEndcapNeg" return a, r1.position, n, m, labels(m);


// get all children nodes of the RootVolume and order them by the "position" value of the "CHILD" relationship; then get the LogVol node connected to the PhysVol nodes, if any (OPTIONAL MATCH)
// Note: if you remove the "OPTIONAL" clause, you only get the PhysVol nodes as children of the RootVolume
match (a:RootVolume)-[r1:CHILD]->(n) with a,r1,n ORDER BY r1.position ASC OPTIONAL MATCH (n)-[r:LOGVOL]->(m:LogVol)  return a, r1.position, n, m, labels(m);

// as above, but get the labels of the children as well
match (a:RootVolume)-[r1:CHILD]->(n) with a,r1,n ORDER BY r1.position ASC OPTIONAL MATCH (n)-[r:LOGVOL]->(m:LogVol)  return a, r1.position, n, labels(n), m, labels(m);

// as above, but get only the PhysVol children (we remove the OPTIONAL clause here) and get the Shape nodes connected to the LogVol nodes
match (a:RootVolume)-[r1:CHILD]->(n) with a,r1,n ORDER BY r1.position ASC MATCH (n)-[r:LOGVOL]->(m:LogVol)-[r2:SHAPE]->(s:Shape)  return a, r1.position, n, labels(n), m, labels(m), s;
