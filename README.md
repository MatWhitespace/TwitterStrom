# Twitter Analysis with Apache Storm

Il progetto comprende tre topologie storm: la prima è un sondaggio per le elezioni presidenziali USA, la seconda estrae delle statistiche rispetto all'epidemia di covid-19 e l'ultima è un analisi genrica di NER su testo.

Tutte le topologie sono basate sull'analisi in stremaing di tweets tramite la libreria twitter4j. Vengono utilizzate anche altre librerie di supporto per la geolocalizzazione, la sentiment analysis e il Name Entity Recognition.

# Run Cluster

Dopo aver installato il cluster apache storm e il server zookeeper ed aver inserito nel path di sistema le directory bin di entrami, esegure su quattro terminali diversi i seguenti comandi:

```bash
zkServer.sh start
zkCli.sh

storm nimbus

storm supervisor

storm ui
```

a questo punto al link [storm ui] (localhost:8080) si potrà visualizzare lo stato del production cluster.

# Run Project

Nella root directory (contente il file pom.xml) assemblare il jar tramite il comando maven:

```bash
mvn assembly:assembly
```

verrà creara una directory target in cui è presente il file jar **TitterStorm-1.0-SNAPSHOT-jar-with-dependencies.jar**, successivemnte spostarsi nella directory target con il comando:

```bash
cd target
```

infine eseguire il run delle topologie con il comando:

```bash
storm jar storm jar TwitterStorm-1.0-SNAPSHOT-jar-with-dependencies.jar main.java.Launcher
```
