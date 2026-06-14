import requests
import json
from confluent_kafka import Producer
import time 
import os

API_URL = ("https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
            "velib-disponibilite-en-temps-reel/records")


def fetch_stations():
    params = {"limit" : 100, "offset" : 0}
    stations = []

    while True:
        for attempt in range(3):
            try:
                response = requests.get(API_URL, params = params, timeout=10)
                response.raise_for_status()
                break
            except requests.RequestException as e :
                print(f"Tentative {attempt +1} /3 échouée : {e}")
                if attempt <2:
                    time.sleep(5)
                else :
                    raise

        data = response.json()

        stations.extend(data["results"])

        if len(stations) >= data["total_count"]:
            break

        params["offset"] += 100
    
    return stations



def build_record(station):
    return {
            "stationcode" : station["stationcode"],
            "name" : station["name"],
            "is_installed" : station["is_installed"],
            "capacity" : station["capacity"],
            "numdocksavailable" : station["numdocksavailable"],
            "numbikesavailable" : station["numbikesavailable"],
            "mechanical" : station["mechanical"],
            "ebike" : station["ebike"],
            "is_renting" : station["is_renting"],
            "is_returning" : station["is_returning"],
            "duedate" : station["duedate"],
            "coordonnees_geo" : station["coordonnees_geo"],
            "nom_arrondissement_communes" : station["nom_arrondissement_communes"],
            "code_insee_commune" : station["code_insee_commune"]}



def send_to_kafka(producer, records):
    for record in records:
        producer.produce( # buffer : accumulate les messages avant de les envoyer en un seul envoi
            topic= "velib-stations",
            key= record["stationcode"],    # permet d'avoir tous les messages d'une même station dans le même partition et dans l'ordre
            value = json.dumps(record).encode("utf-8") # valeur qui est sérialisée en JSON puis encodée en bytes
        )                                              # puisque Kafka attend des bytes

    producer.flush() # flush : force l'envoi des messages bufferisés



if __name__ == "__main__":
    producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")})
    
    while True : 
        try :
            stations = fetch_stations()
            records = [build_record(s) for s in stations]
            send_to_kafka(producer, records)
            print(f"{len(records)} messages envoyés")
        except Exception as e :
            print(f"Erreur : {e}")
        time.sleep(60)

