FROM apache/spark:3.5.0-python3

#1. télécharge le connecteur Kafka pour Spark Streaming : lie Spark avec Kafka
#2. télécharge les clients Kafka : client qui permet d'envoyer des données à Kafka
#3. télécharge le provider de token Kafka : gère l'authentification entre Spark et Kafka
#4. télécharge le pool de workers Kafka : gère la connexion entre Spark et Kafka

RUN curl -o /opt/spark/jars/spark-sql-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar && \

    
    curl -o /opt/spark/jars/kafka-clients.jar \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar && \

    
    curl -o /opt/spark/jars/spark-token-provider-kafka.jar \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar && \

    
    curl -o /opt/spark/jars/commons-pool2.jar \ 
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

USER root
# par défaut spark est lancé avec l'utilisateur spark qui n'a pas les droits
# pour pouvoir installer les dépendances il faut passer en root

RUN pip install opensearch-py


# définit le répertoire de travail
WORKDIR /app

# copie le script Python dans le répertoire de travail
COPY main.py . 

# exécute le script Python
CMD ["/opt/spark/bin/spark-submit", "main.py"] 