
import redis
import redis_graph_common
import re

class Query_Configuration():

   def __init__( self, redis, redis_graph_common):
        self.redis   = redis
        self.common  = redis_graph_common

      

   def match_labels( self, label, starting_path = None ):
       temp_list =  self.common.match( "*", label, "*" , starting_path)
       return_value = []
       for i in temp_list:
           
           temp_list =  i.split(self.common.sep )
           
           m_relationship,m_label,m_name = self.common.reverse_string_key( temp_list[-1] )
           if label == m_label:
              return_value.append(i)
       return return_value

   def match_relationship( self, relationship, starting_path = None):
       temp_list =  self.common.match( relationship, "*", "*", starting_path  )
       return_value = []
       for i in temp_list:
           
           temp_list =  i.split(self.common.sep )
           
           m_relationship,m_label,m_name = self.common.reverse_string_key( temp_list[-1] )
           if relationship == m_relationship:
              return_value.append(i)
       return return_value



   def match_label_property( self, label, prop_key, prop_value, starting_path = None):
       return_value = []
       results = self.match_labels( label, starting_path  )
       
       for i in results:
          
          if self.redis.hexists(i,prop_key) == True:
              if self.redis.hget(i,prop_key) == prop_value:
                  return_value.append(i)

       return return_value


   

   def modify_properties( self, redis_key, new_properties):
       for i in new_properties.keys():
         redis.hset(redis_key,i, new_properties[i] )
     

   def match_label_property_specific( self, label_name, property_name, property_value, label, return_name, return_prop_value):
       return_value = []
       # first step
       first_step = self.match_label_property( label_name, property_name, property_value)
       for i in first_step:
           
           results = self.match_label_property( label, return_name, return_prop_value,i)
           return_value.extend(results)
       return return_value

   def match_label_property_generic( self, label_name, property_name, property_value, label ):
       return_value = []
       # first step
       first_step = self.match_label_property( label_name, property_name, property_value)
       for i in first_step:
           results = self.match_labels( label,i)
           return_value.extend(results)
       return return_value


 
   def match_relationship_property( self, relationship, prop_key, prop_value, starting_path = None):
       return_value = []
       results = self.match_relationship( relationship, starting_path  )
       
       for i in results:
          
          if self.redis.hexists(i,prop_key) == True:
              if self.redis.hget(i,prop_key) == prop_value:
                  return_value.append(i)

       return return_value


   def match_relationship_property_specific( self, relationship, property_name, property_value, label, return_name, return_prop_value):
       return_value = []
       # first step
       first_step = self.match_relationship_property( relationship, property_name, property_value)
       for i in first_step:
           
           results = self.match_label_property( label, return_name, return_prop_value,i)
           return_value.extend(results)
       return return_value

   def match_relationship_property_generic( self, relationship, property_name, property_value, label ):
       return_value = []
       # first step
       first_step = self.match_relationship_property( relationship, property_name, property_value)
       for i in first_step:
           results = self.match_labels( label,i)
           return_value.extend(results)
       return return_value
       
'''
nicole = graph.merge_one('Person', 'name', 'Nicole')
nicole['hair'] = 'blonde'
Then you need to push those changes to the graph; cast is inappropriate for updating properties on something that is already a py2neo Node object:

nicole.push()

if __name__ == "__main__" :
   pass
   # concept of namespace name is a string which ensures unique name
   # the name is essentially the directory structure of the tree
   def construct_merge_node(self, push_namespace,relationship, label, name, new_properties ):
       namespace = self.get_namespace(name)
       node = self.graph.find_one(label ,property_key="name",property_value=name) 
       if self.graph.find_one(label ,property_key="name",property_value=name) != None:
           for i in properties.keys():
               node.properties[i]=properties[i]
               node.push()
           return node
       else:
           node = Node(label)
           node.properties["namespace"]=namespace
           node.properties["name"] = name
           for i in properties.keys():
               node.properties[i]=properties[i]
           self.graph.create(node)
           if len(self.namespace) !=0:
               relation_enity = Relationship( self.get_namespace_node(),relationship,node) 
           self.graph.create( relation_enity )
           if push_namespace == True:
               self.namespace.append(name)
               self.namespace.append(node)
           return node

   # not tested yet
   def cypher_query( self, query_string, return_variable ):
       query_string = query_string + "   RETURN "+return_variable  
       #print "---------query string ---------------->"+query_string
       results =  self.graph.cypher.execute(query_string)
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value
   def match_relation_property_specific( self,label_name, property_name, property_value, label,return_name,return_value ):
       query_string = "MATCH (n:"+label_name+'   { '+property_name +':"'+property_value+'"})-[*]->(o:'+label+') Where o.'+return_name+' = "'+return_value +'" RETURN o'
       #print "query string ",query_string  
       results =  self.graph.cypher.execute(query_string)
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value
   def match_relation_property( self, label_name, property_name, property_value, label ):
       query_string = "MATCH (n:"+label_name+'   { '+property_name +':"'+property_value+'"})-[*]->(o:'+label+')   RETURN o'  
       results =  self.graph.cypher.execute(query_string)
       return_value = []
       for i in results:
           return_value.append(i[0])
       return return_value

'''
