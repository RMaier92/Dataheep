from time import sleep
import pytest
from dataheep import Dataheep
import pandas as pd
import numpy as np
import os

@pytest.fixture
def dataheep():
    return Dataheep.object_create()

"""
Tested functions:
- attribute_add (not value, only attribute)
- attribute_remove (not value, only attribute)
- object_info

Tested objects:
- only a single object
"""
def test___object_info(dataheep):
    errors = []
    
    dataheep = Dataheep.attribute_add(dataheep, "bronze", 123)
    if not ( sorted(Dataheep.object_info(dataheep)["Attributes"]) == ["bronze"] ):
        errors.append(f"Object info not correct")

    dataheep = Dataheep.attribute_add(dataheep, "silver")
    if not ( sorted(Dataheep.object_info(dataheep)["Attributes"]) == ["bronze", "silver"] ):
        errors.append(f"Object info not correct")

    dataheep = Dataheep.attribute_remove(dataheep, "silver")
    if not ( sorted(Dataheep.object_info(dataheep)["Attributes"]) == ["bronze"] ):
        errors.append(f"Object info not correct")

    dataheep = Dataheep.attribute_remove(dataheep, "bronze")
    if not ( sorted(Dataheep.object_info(dataheep)["Attributes"]) == [] ):
        errors.append(f"Object info not correct")

    assert not errors, "errors occured:\n{}".format("\n".join(errors))

"""
Tested functions:
- attribute_add (value + attribute)
- attribute_remove (value + attribute)
"""
def test___attribute_get_set(dataheep: Dataheep):
    errors = []

    dataheep = Dataheep.attribute_add(dataheep, "bronze", 123)
    if not ( dataheep.bronze == 123 ):
        errors.append(f"Attribute bronze not correct")

    dataheep = Dataheep.attribute_add(dataheep, "silver")
    dataheep.silver = "Hello World"
    if not ( dataheep.silver == "Hello World" ):
        errors.append(f"Attribute bronze not correct")

    
    dataheep = Dataheep.attribute_add(dataheep, "gold")
    dataheep.gold = "Testval"
    if not (Dataheep.attribute_get(dataheep, "gold") == "Testval"):
        errors.append(f"Attribute bronze not correct")
    

    dataheep = Dataheep.attribute_add(dataheep, "platin")
    Dataheep.attribute_set(dataheep, "platin", "Testvalue_platin")
    if not (dataheep.platin == "Testvalue_platin"):
        errors.append(f"Attribute bronze not correct")

    assert not errors, "errors occured:\n{}".format("\n".join(errors))

def test___attribute_info(dataheep: Dataheep):
    errors = []
    dataheep = Dataheep.attribute_add(dataheep, "platin")

    attribute_info = Dataheep.attribute_info(dataheep, "platin")
    

    if not (
        attribute_info["value_type"] == type(None) and
        attribute_info["created"] == attribute_info["lastUpdated"]
    ):
        errors.append(f"Attribute bronze not correct")
    
    sleep(0.01)


    dataheep.platin = 234
    attribute_info = Dataheep.attribute_info(dataheep, "platin")
    
    if not (
        attribute_info["value_type"] == type(12) 
    ):
        errors.append(f"Attribute bronze not correct")
    

    assert not errors, "errors occured:\n{}".format("\n".join(errors))

def test___multipleDboxes():
    dataheep1 = Dataheep.object_create()
    dataheep2 = Dataheep.object_create()
    errors = []
    
    dataheep1 = Dataheep.attribute_add(dataheep1, "bronze_dataheep1")
    dataheep1 = Dataheep.attribute_add(dataheep1, "silver_dataheep1", 395)
    dataheep1.bronze_dataheep1 = 77

    dataheep2 = Dataheep.attribute_add(dataheep2, "yellow_dataheep2", "Hello World")
    dataheep2 = Dataheep.attribute_add(dataheep2, "green_dataheep2")

    if not ( sorted(Dataheep.object_info(dataheep1)["Attributes"]) == ["bronze_dataheep1", "silver_dataheep1"] ):
        errors.append(f"TEST 1 - Object info not correct")

    if not ( sorted(Dataheep.object_info(dataheep2)["Attributes"]) == ["green_dataheep2", "yellow_dataheep2"] ):
        errors.append(f"TEST 2 - Object info not correct")


    assert not errors, "errors occured:\n{}".format("\n".join(errors))

def test___load_save():
    errors = []
    dataheep_obj_path = "./test.json"

    values_test = {
        "value_1": 573,
        "value_2": "hello World!!",
        "value_3": False,
        "value_4": 3.23,
        
        "value_5": {
            "value_5_dict_1": 12,
            "value_5_dict_2": "hello",
            "value_5_dict_3": 12,
        },
        
        "value_6": pd.DataFrame({"A":[1,5,7,8],
                  "B":[5,8,4,3],
                  "C":[10,4,9,3]})
        
    }

    dataheep1 = Dataheep.object_create()

    for key, value in values_test.items():
        Dataheep.attribute_add(dataheep1, key, value)

    Dataheep.object_save(dataheep1, dataheep_obj_path)
    

    dataheep_loaded = Dataheep.object_load(dataheep_obj_path)

    for key, value in values_test.items():
        if type(value) == pd.DataFrame:
            #pass
            if not value.equals(Dataheep.attribute_get(dataheep_loaded, key)):
                errors.append(f"Error in {key}")

        else:
            if not ( value == Dataheep.attribute_get(dataheep_loaded, key) ):
                errors.append(f"Error in {key}")

    os.remove("./test.json")
    assert not errors, "errors occured:\n{}".format("\n".join(errors))
    