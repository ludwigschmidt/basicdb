from abc import ABC, abstractmethod

from .basicdb import Object, Blob, Relationship
 
class DBAdapter(ABC):
    @abstractmethod
    def insert_object(self,
                      namespace,
                      name,
                      type_,
                      username,
                      json_data,
                      binary_data,
                      return_result):
        pass
    
    @abstractmethod
    def get_object(self,
                   uuid,
                   uuids,
                   namespace,
                   name,
                   names,
                   type_,
                   include_hidden,
                   return_blobs,
                   first_object,
                   second_object,
                   assert_exists):
        pass