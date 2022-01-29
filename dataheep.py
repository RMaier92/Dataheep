from __future__ import annotations
from datetime import datetime
from time import sleep
import pandas as pd
from pathlib import Path
import os
import json
import numpy as np


class DataheepBackend:
    """[summary]
    """
    def __init__(self) -> None:
        self._var_registry = {
            "config": {
                "autosave": True
            },
            "data_properties": {}
        }

    def __object_serialize(self, var_registry: str) -> str:
        """[summary]

        Args:
            var_registry (str): [description]

        Raises:
            NotImplementedError: [description]
            NotImplementedError: [description]

        Returns:
            str: [description]
        """

        _var_registry_serialized = {
            "config": var_registry["config"],
            "data_properties": {}
        }

        for key, content in var_registry["data_properties"].items():
            
            _var_registry_serialized["data_properties"][key] = var_registry["data_properties"][key]
            _var_registry_serialized["data_properties"][key]["type"] = str(type( content["value"] ).__name__)
        
            if(   type(content["value"]) in [str, int, bool, float, dict] ):
                pass

            elif(   type(content["value"]) in [complex, list, tuple] ):
                raise NotImplementedError

            elif( type(content["value"]) in [pd.DataFrame] ):                
                _var_registry_serialized["data_properties"][key]["value"] = content["value"].to_json()

            elif( content["value"] == None ):   
                pass
            else:
                raise NotImplementedError( f"Datatype currently not supported from Dataheep: {type(content['value'])}")

        return _var_registry_serialized

    def object_save(self, file_path: Path) -> None:
        """[summary]

        Args:
            file_path (Path): [description]
        """

        serialized_var_registry = self.__object_serialize(self._var_registry)

        with open( file_path, "w" ) as file_ref:
            json.dump( serialized_var_registry, file_ref )

    def object_load(self, file_path: Path) -> None:
        """[summary]

        Args:
            file_path (Path): [description]
        """

        with open( Path(file_path), "r") as f:
            _var_registry_raw = json.load(f) 
            
        self._var_registry = self.__object_deserialize(_var_registry_raw)

    def __object_deserialize(self, serialized_str: str) -> None:
        """[summary]

        Args:
            serialized_str (str): [description]

        Raises:
            NotImplementedError: [description]

        Returns:
            [type]: [description]
        """

        for key, content in serialized_str["data_properties"].items():
            
            if( content["type"] == "str" ):
                serialized_str["data_properties"][key]["value"] = str( content["value"] )

            elif( content["type"] == "int" ):
                serialized_str["data_properties"][key]["value"] = int( content["value"] )

            elif( content["type"] == "bool" ):
                serialized_str["data_properties"][key]["value"] = bool( content["value"] )
            
            elif( content["type"] == "float" ):
                serialized_str["data_properties"][key]["value"] = float( content["value"] )
            
            elif( content["type"] == "dict" ):
                serialized_str["data_properties"][key]["value"] = dict( content["value"] )
            
            elif( content["type"] == "DataFrame"):
                serialized_str["data_properties"][key]["value"] = pd.read_json(content["value"])

            else:
                raise NotImplementedError( f"Datatype not implemented from Dataheep: {content['type']}")
        return serialized_str

    def valid_obj_path(self, file_path: Path) -> bool:
        """[summary]

        Args:
            file_path (Path): [description]

        Returns:
            bool: [description]
        """

        return os.path.exists( os.path.dirname( file_path ) )

    def get_attribute(self, name):
        """[summary]

        Args:
            name ([type]): [description]

        Raises:
            ValueError: [description]

        Returns:
            [type]: [description]
        """

        if name in self._var_registry["data_properties"].keys():
            return self._var_registry["data_properties"][name]["value"]
        else:
            raise ValueError("Value not there")

    def set_attribute(self, name: str, value: any) -> None:
        """[summary]

        Args:
            name (str): [description]
            value (any): [description]

        Raises:
            ValueError: [description]
        """

        if name in self._var_registry["data_properties"].keys():
            self._var_registry["data_properties"][name]["value"] = value
            self._var_registry["data_properties"][name]["lastUpdated"] = str(datetime.now())
        else:
            raise ValueError("Value not there")

    def add_attribute(self, name: str, value: any) -> None:
        """[summary]

        Args:
            name (str): [description]
            value (any): [description]

        Raises:
            ValueError: [description]
        """

        if name in self._var_registry["data_properties"].keys():
            raise ValueError("Value already there")
        else:
            self._var_registry["data_properties"][name] = {
                "value": value,
                "created": str(datetime.now()),
                "lastUpdated": str(datetime.now())
            }

    def remove_attribute(self, name: str) -> None:
        """[summary]

        Args:
            name (str): [description]

        Raises:
            ValueError: [description]
        """

        if name not in self._var_registry["data_properties"].keys():
            raise ValueError("Value not there. Can't be removed")
        else:
            del self._var_registry["data_properties"][name]    

    def info_object(self):
        """[summary]

        Returns:
            [type]: [description]
        """

        return {
            "Attributes": list(self._var_registry["data_properties"].keys())   
        }
    
    def attribute_info(self, name: str) -> None:
        """[summary]

        Args:
            name (str): [description]

        Raises:
            ValueError: [description]

        Returns:
            [type]: [description]
        """

        if name in self._var_registry["data_properties"].keys():
            return {
                "value_type": type( self._var_registry["data_properties"][name]["value"] ),
                "created": self._var_registry["data_properties"][name]["created"],
                "lastUpdated": self._var_registry["data_properties"][name]["lastUpdated"]
            }
            
        else:
            raise ValueError("Value not there. Can't be removed")


class Dataheep:
    """[summary]
    """

    def __getattribute__(self, __name: str) -> any:
        """[summary]

        Args:
            __name (str): [description]

        Returns:
            any: [description]
        """

        if __name == "backend":
            return super(Dataheep,self).__getattribute__(__name)
        else:
            return self.backend.get_attribute(__name)

    def __setattr__(self, name, value):
        """[summary]

        Args:
            name ([type]): [description]
            value ([type]): [description]
        """

        if name == "backend":
            super(Dataheep, self).__setattr__(name, value)
        else:
            self.backend.set_attribute(name, value)

    @staticmethod
    def object_create() -> Dataheep:
        """[summary]

        Returns:
            Dataheep: [description]
        """

        obj_ref = Dataheep.__new__(Dataheep)
        obj_ref.backend = DataheepBackend()
        return obj_ref

    @staticmethod
    def object_save(obj_ref: Dataheep, file_path: str = None) -> None:
        """[summary]

        Args:
            obj_ref (Dataheep): [description]
            file_path (str, optional): [description]. Defaults to None.

        Raises:
            ValueError: [description]
            ValueError: [description]
        """

        if obj_ref.backend.valid_obj_path(file_path):
            try:
                obj_ref.backend.object_save(file_path)
            except:
                raise ValueError("Saving not successful")
        else:
            raise ValueError("Path not available")

    @staticmethod
    def object_load(file_path: Path) -> Dataheep:
        """[summary]

        Args:
            file_path (Path): [description]

        Returns:
            Dataheep: [description]
        """

        obj_ref = Dataheep.__new__(Dataheep)
        obj_ref.backend = DataheepBackend()

        obj_ref.backend.object_load(file_path)
        return obj_ref

    @staticmethod
    def object_info(obj_ref: Dataheep) -> any:
        """[summary]

        Args:
            obj_ref (Dataheep): [description]

        Returns:
            any: [description]
        """
        
        return obj_ref.backend.info_object()

    @staticmethod
    def attribute_add(obj_ref: Dataheep,  name: str, value: any = None ) -> any:
        """[summary]

        Args:
            obj_ref (Dataheep): [description]
            name (str): [description]
            value (any, optional): [description]. Defaults to None.

        Returns:
            any: [description]
        """

        obj_ref.backend.add_attribute(name, value)
        return obj_ref

    @staticmethod
    def attribute_remove(obj_ref: Dataheep,  name: str) -> any:
        """[summary]

        Args:
            obj_ref (Dataheep): [description]
            name (str): [description]

        Returns:
            any: [description]
        """

        obj_ref.backend.remove_attribute(name)
        return obj_ref

    @staticmethod
    def attribute_set(obj_ref: Dataheep,  name: str, value: any) -> None:
        """[summary]

        Args:
            obj_ref (Dataheep): [description]
            name (str): [description]
            value (any): [description]
        """

        obj_ref.backend.set_attribute(name, value)

    @staticmethod
    def attribute_get(obj_ref: Dataheep,  name: str) -> any:
        """[summary]

        Args:
            obj_ref (Dataheep): [description]
            name (str): [description]

        Returns:
            any: [description]
        """

        print(name)
        return obj_ref.backend.get_attribute(name)

    @staticmethod
    def attribute_info(obj_ref: Dataheep,  name: str) -> any:
        """[summary]

        Args:
            obj_ref (Dataheep): [description]
            name (str): [description]

        Returns:
            any: [description]
        """
        return obj_ref.backend.attribute_info(name)

    @staticmethod
    def object_autosave(enabled: bool, file_path: Path, parallel_save: bool = False) -> None:
        """[summary]

        Args:
            enabled (bool): [description]
            file_path (Path): [description]
            parallel_save (bool, optional): [description]. Defaults to False.

        Raises:
            NotImplementedError: [description]
        """

        raise NotImplementedError

    @staticmethod
    def object_history(obj_ref: Dataheep) -> any:
        """[summary]

        Args:
            obj_ref (Dataheep): [description]

        Raises:
            NotImplementedError: [description]

        Returns:
            any: [description]
        """

        raise NotImplementedError
    
    @staticmethod
    def attribute_history(obj_ref: Dataheep) -> any:
        """[summary]

        Args:
            obj_ref (Dataheep): [description]

        Raises:
            NotImplementedError: [description]

        Returns:
            any: [description]
        """
        
        raise NotImplementedError



dataheep = Dataheep.object_create()