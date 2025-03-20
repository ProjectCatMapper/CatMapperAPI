# Admin Help File

# Upgrading Neo4j databases

Instructions can be found here:
https://neo4j.com/docs/upgrade-migration-guide/current/version-5/upgrade-minor/standalone/docker-example/

Assuming the current version of the database is neo4j:5.11.0-community

First, update the docker-compose.yml to the new version of the database

run the following command to stop the current database (assuming we are updating archamap)
and then remove it

```bash
docker stop archamap1
docker rm archamap1
```

then run the following command to launch the
```

then run the following command to launch the new container

```bash 
docker-compose up -d --build archamap1
```

# updating custom cypher functions

To update custom cypher functions, modify the custom cypher functions file at 

```bash
/mnt/storage/app/customCypherFunctions.cypher
```

then run the following command to copy the file to the neo4j containers and restart them

```bash
sudo cp customCypherFunctions.cypher db/sociomap1/import/
sudo cp ustomCypherFunctions.cypher db/archamap1/import/
docker restart sociomap1
docker restart archamap1
```

the database is set to automatically run the custom cypher functions file on startup with these settings

apoc.import.file.enabled=true

in 

```bash
/mnt/storage/app/db/sociomap1/conf/apoc.conf
/mnt/storage/app/db/archamap1/conf/apoc.conf
```

make sure to restart the databases after applying changes

```bash
docker restart sociomap1
docker restart archamap1
```

to update the startup.sh script for the neo4j docker containers, update the bash script at

```bash
/mnt/storage/app/startup.sh
```

then run the following command to copy the file to the neo4j containers and restart them

```bash
sudo cp /mnt/storage/app/startup.sh db/sociomap1/import/
sudo cp /mnt/storage/app/startup.sh db/archamap1/import/
docker restart sociomap1
docker restart archamap1
```



