# flink-stream-enrichment
Flink Stream enrichment using data stored in State

Parameter for the flink job:
1. brokers
2. stream topic name
3. lookup topic name
4. enriched topic name
5. stream topic consumer group
6. lookup topic consumer group
7. stream topic key
8. lookup topic key

Command to run flink job for first time:

cd FLINK/bin

./flink run --detached -p 6 -m yarn-cluster -ynm <flink_job_name> -ys 2 -yjm 2048mb -ytm 3048m -yD security.kerberos.login.keytab=./service_user.keytab -yD security.kerberos.login.principal=service_user -c com.cloudera.flink.enrichments.StreamEnrichmentJob ./flink-stream-enrichment-0.0.2-SNAPSHOT.jar broker1:9093,broker2:9093,broker2:9093 stream_topic lookup_topic enriched_topic stream_topic_cg lookup_topic_cg stream_key lookup_key

Command to run flink job from last checkpoint:

./flink run -s s3a://flink_last_checkpoint_dir --detached -p 6 -m yarn-cluster -ynm <flink_job_name> -ys 2 -yjm 2048mb -ytm 3048m -yD security.kerberos.login.keytab=./service_user.keytab -yD security.kerberos.login.principal=service_user -c com.cloudera.flink.enrichments.StreamEnrichmentJob ./flink-stream-enrichment-0.0.2-SNAPSHOT.jar broker1:9093,broker2:9093,broker2:9093 stream_topic lookup_topic enriched_topic stream_topic_cg lookup_topic_cg stream_key lookup_key
