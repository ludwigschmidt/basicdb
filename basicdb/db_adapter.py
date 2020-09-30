from abc import ABC, abstractmethod


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
                   relationship_first,
                   relationship_second,
                   relationship_type,
                   assert_exists):
        pass
    
    @abstractmethod
    def delete_objects(self,
                       uuids,
                       hide_only,
                       check_namespace,
                       namespace_to_check):
        pass

    @abstractmethod
    def insert_blob(self,
                    *,
                    object_identifier,
                    name,
                    type_,
                    username,
                    json_data,
                    serialization,
                    size,
                    return_result,
                    check_namespace,  # check whether the object belongs to the given namespace
                    namespace_to_check):
        pass

    @abstractmethod
    def get_blobs(self,
                  *,
                  object_identifier,
                  match_name,
                  name,
                  uuid,
                  uuids,
                  include_hidden,
                  check_namespace,    # check whether the object belongs to the given namespace
                  namespace_to_check,
                  assert_exists):
        pass

    @abstractmethod
    def delete_blobs(self,
                     uuids,
                     hide_only,
                     check_namespace,
                     namespace_to_check):
        pass
    
    @abstractmethod
    def insert_relationship(self,
                            *,
                            first,
                            second,
                            type_,
                            return_result,
                            check_namespace,  # check whether the object belongs to the given namespace
                            namespace_to_check):
        pass

    @abstractmethod
    def get_relationships(self,
                          *
                          uuid,
                          uuids,
                          first,
                          second,
                          type_,
                          namespace,
                          filter_namespace,
                          include_hidden,
                          assert_exists):
        pass
    
    @abstractmethod
    def delete_relationships(self,
                             uuids,
                             hide_only,
                             check_namespace,
                             namespace_to_check):
        pass