#
#  The purpose of this file is to load a system configuration
#  in the graphic data base
#

import json

import redis
from redis_graph_common   import Redis_Graph_Common
from redis_graph_populate import Build_Configuration
from redis_graph_common   import Redis_Graph_Common
from farm_template        import Construct_Farm
from redis_graph_query   import Query_Configuration
import copy
 
if __name__ == "__main__" :
   redis  = redis.StrictRedis( host = "127.0.0.1", port=6379, db = 11 )   
   common = Redis_Graph_Common( redis)
   common.delete_all()
   qc = Query_Configuration( redis, common )
   bc = Build_Configuration(redis,common)
   cf = Construct_Farm(bc)
   
   #
   #
   # Construct Systems
   #
   #
   cf.construct_system("LaCima Operations")

   #
   #
   # Construction Sites for LaCima
   #
   #

   cf.construct_site( name="LaCima",wired=True,address="21005 Paseo Montana Murrieta, Ca 92562")

   #
   #  Constructing Controllers
   #
   #
   #
   cf.construct_controller( "PI_1","rpc_queue","alert_status_queue","192.168.1.82","irrigation/1","LaCima",
   {"temperature":"Main Controller Temperature",
    "ping":"Main Controller Connectivity",
    "irrigation_resets":"Main Controller Irrigation Resets",
    "system_resets":"Main Controller System Resets"},
    "CONTROLLER_STATUS")

   cf.add_event_queue( "cloud_alarm_queue",
                       { 
                         "OPEN_MASTER_VALVE": {"card":"MANUAL_OPERATIONS","label":"yellow" },
                         "CLOSE_MASTER_VALVE":{"card":"MANUAL_OPERATIONS","label":"yellow" },
                         "DIAGNOSTICS_SCHEDULE_STEP_TIME":{"card":"MANUAL_OPERATIONS","label":"yellow" },
                         "RESUME_OPERATION":{"card":"MANUAL_OPERATIONS","label":"yellow" },
                         "OFFLINE":{"card":"MANUAL_OPERATIONS","label":"yellow" },
                         "SKIP_STATION":{"card":"MANUAL_OPERATIONS","label":"yellow" },
                         "IRRIGATION:CURRENT_ABORT":{"card":"ABORT_OPERATIONS","label":"red" },
                         "IRRIGATION:FLOW_ABORT":{"card":"ABORT_OPERATIONS","label":"red" },
                         "CHECK_OFF":{"card":"Check Off","label":"fromevent" },
                         "CLEAN_FILTER":{"card":"Clean Filter","label":"green" }
                       })

   cf.add_event_queue( "QUEUES:CLOUD_ALARM_QUEUE",
                        { 
                          "store_eto": {"card":"ETO History","label":"green" },
                          "reboot": {"card":"Reset History","label":"red" },
                        } )

   cf.add_diagnostic_card_header()
   org_name =    "LaCima Ranch"
   list_name =   "PI_1 Irrigation Controller"
   board_name  = "System Operation"
   cf.add_diagnostic_card(org_name,board_name,list_name,"Main Controller Temperature" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Main Controller Connectivity" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Main Controller Irrigation Resets" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Main Controller System Resets" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Reset History")
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 1 Connectivity" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 2 Connectivity" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 3 Connectivity" )
   board_name  = "Irrigation Electrical Wiring"
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 1 Open Wire" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 2 Open Wire" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 3 Open Wire" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 1 Shorted Selenoid" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 2 Shorted Selenoid" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Remote 3 Shorted Selenoid" )

   board_name  = "Irrigation Plumbing"
   cf.add_diagnostic_card(org_name,board_name,list_name,"ABORT_OPERATIONS")
   cf.add_diagnostic_card(org_name,board_name,list_name,"ETO History" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"MANUAL_OPERATIONS" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Clean Filter" )
   cf.add_diagnostic_card(org_name,board_name,list_name,"Check Off"  )

 
   board_name = "Irrigation Schedules"
   for i in range(1,12):
       cf.add_diagnostic_card(org_name,board_name,list_name,"fruit_trees_low_water:"+str(i) )
   for i in range(1,6):
       cf.add_diagnostic_card(org_name,board_name,list_name,"house:"+str(i) )
   for i in range(1,16):
       cf.add_diagnostic_card(org_name,board_name,list_name,"flowers:"+str(i) )
   for i in range( 1,7):
       cf.add_diagnostic_card(org_name,board_name,list_name,"clean-filter:"+str(i) )
   cf.end_diagnostic_card_header()
       

   cf.add_flow_sensor_header()
   cf.add_flow_sensor(name='main_sensor',controller='satellite_1',io=1,conversion_factor = 0.0224145939)
   cf.end_flow_sensor_header(  )

   # need to automatically construct these files in the future
   cf.add_schedule_header()
   cf.add_schedule( name='fruit_trees_low_water',number=11,flow_sensor_names = ['main_sensor'], card_link = "fruit_trees_low_water:" )
   cf.add_schedule( name='flowers',number=14,flow_sensor_names = ['main_sensor'], card_link = "flowers:")
   cf.add_schedule( name='clean-filter',number=6,flow_sensor_names = ['main_sensor'], card_link = "clean-filter:")
   cf.add_schedule( name='house',number=5,flow_sensor_names = ['main_sensor'], card_link = "house:")
   cf.end_schedule_header()

   

   #
   #  Contructing IO Devices
   #  a remote has to be attached to a controller
   #  Multiple controllers can interface to udp server but not to same controller
   #

   cf.add_udp_io_sever(name="main_remote", ip = "192.168.1.82", redis_key="MODBUS_STATISTICS:127.0.0.1",remote_type= "UDP", port=5005   )
   cf.add_rtu_interface(name = "rtu_2",protocol="modify_modbus",baud_rate=38400 )
   cf.add_remote(  name="satellite_1",modbus_address=100,irrigation_station_number=44, card_dict={"open":"Remote 1 Open Wire","short":"Remote 1 Shorted Selenoid","connectivity":"Remote 1 Connectivity"})
   cf.add_remote(  name="satellite_2",modbus_address=125 ,irrigation_station_number=22,card_dict={"open":"Remote 2 Open Wire","short":"Remote 2 Shorted Selenoid","connectivity":"Remote 2 Connectivity"})
   cf.add_remote(  name="satellite_3",modbus_address=170,irrigation_station_number=22,card_dict={"open":"Remote 3 Open Wire","short":"Remote 3 Shorted Selenoid","connectivity":"Remote 3 Connectivity"}) 
   cf.end_rtu_interface()
   cf.end_udp_io_server()
   cf.end_controller()
   cf.end_site()
   cf.end_system()
   keys = redis.keys("*")
   
   for i in keys:
      print "+++++++++++++:"
      print i
      temp = i.split( common.sep)
      print len(temp)
      print redis.hgetall(i)
      print "----------------"
   print "lenght",len(keys)
   print "testing query functions"
   
   print qc.match_labels( "CONTROLLER" ) # match single item
   temp = qc.match_labels( "REMOTE" ) # match single item
   print len(temp),temp

   print qc.match_relationship( "CONTROLLER" ) # match single item
   temp = qc.match_relationship( "REMOTE" ) # match single item
   print len(temp),temp

   temp = qc.match_label_property( "REMOTE", "name", "satellite_1")
   print len(temp),temp

   temp= qc.match_label_property_specific( "CONTROLLER", "name", "PI_1", "REMOTE", "name", "satellite_1")
   print len(temp),temp

   temp = qc.match_label_property_generic(  "CONTROLLER", "name", "PI_1", "REMOTE" )
   print len(temp),temp

   temp= qc.match_relationship_property_specific( "CONTROLLER", "name", "PI_1", "REMOTE", "name", "satellite_1")
   print len(temp),temp

   temp = qc.match_relationship_property_generic(  "CONTROLLER", "name", "PI_1", "REMOTE" )
   print len(temp),temp

   common.delete_all()
