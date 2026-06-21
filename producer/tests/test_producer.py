import os 
import sys 


sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from main import build_record

def make_fake_station():
    return {
            "stationcode" : "12345",
            "name" : "Velib Montparnasse",
            "is_installed" : "OUI",
            "capacity" : 20,
            "numdocksavailable" : 5,
            "numbikesavailable" : 15,
            "mechanical" : 10,
            "ebike" : 5,
            "is_renting" : "OUI",
            "is_returning" : "OUI",
            "duedate" : "2024-01-15T10:30:00+00:00",
            "coordonnees_geo" : {"lat": 48.8418, "lon": 2.3214},
            "nom_arrondissement_communes" : "Paris 14e Arrondissement",
            "code_insee_commune" : "75114"}


def test_build_record_return_14_field():
    station = make_fake_station()
    record = build_record(station)
    assert len(record) == 14

def test_build_record_valeurs_correctes():
    station = make_fake_station()
    record = build_record(station)
    assert record["stationcode"] == "12345"
    assert record["capacity"] == 20
    assert record["nom_arrondissement_communes"] == "Paris 14e Arrondissement"


def test_build_record_missing_field():
    station = make_fake_station()
    del station["capacity"]
    try:
        build_record(station)
        assert False, "Doit lever un erreur"
    except KeyError:
        pass
    