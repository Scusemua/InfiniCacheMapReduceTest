import time 
import datetime 
import plotly.figure_factory as ff
import plotly as py 
import plotly.graph_objects as go
import pandas as pd
import math
import argparse 
import datetime 
from colour import Color
from random import randint, uniform

class Entry(object):
   def __init__(self, start, end, size):
      self.start = start 
      self.end = end
      self.size = size 
   
   def is_active(self, current):
      return current >= self.start and current < self.end

def is_active(current, start, end):
   if current >= start and current < end:
      return True 
   return False 

def calculate_working_set_size(time, objects):
   sum = 0
   for entry in objects:
      active = entry.is_active(time)
      if active:
         sum += entry.size 
   
   return sum 

if __name__ == "__main__":
   parser = argparse.ArgumentParser()
   parser.add_argument("-mf", "--map_file", dest = "map_file", type = str, default = "merged_map.dat")
   parser.add_argument("-rf", "--reduce_file", dest = "reduce_file", type = str, default = "merged_reduce.dat")
   
   args = parser.parse_args()
   
   print("Reading map data...")
   
   with open(args.map_file) as f:
      map_lines = f.readlines()
      
   print("Reading reduce data...")
   
   with open(args.reduce_file) as f:
      reduce_lines = f.readlines()
   
   data = dict()
   
   start_times = list()
   sizes = set()
   print("Processing map data...")
   df = []
   for line in map_lines:
      line = line.replace('}', '')
      line = line.replace('{', '')
      splits = line.split(" ")
      task_num = splits[0]
      redis_key = splits[1]
      bytes_written = float(splits[2])
      start = float(splits[3][0:10] + "." + splits[3][10:-2])
      end = float(splits[4][0:10] + "." + splits[4][10:-2])
      sizes.add(bytes_written)
      start_times.append(start)
      data[redis_key] = {
         "start": start,
         "size": bytes_written
      }
      
      var = dict(
         Task = "Mapper " + str(task_num), 
         Start = datetime.datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S.%f'), 
         Finish = datetime.datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S.%f'), 
         Complete = bytes_written)
      df.append(var)
   
   start_times.sort()
   end_times = list()
   entries = list() 
   
   print("Processing reduce data...")
   for line in reduce_lines:
      line = line.replace('}', '')
      line = line.replace('{', '')   
      splits = line.split(" ")
      task_num = splits[0]
      redis_key = splits[1]
      bytes_read = float(splits[2])
      start = float(splits[3][0:10] + "." + splits[3][10:-2])
      end = float(splits[4][0:10] + "." + splits[4][10:-2])
      sizes.add(bytes_read)
      end_times.append(end)
      var = dict(
         Task = "Reducer " + str(task_num), 
         Start = datetime.datetime.fromtimestamp(start).strftime('%Y-%m-%d %H:%M:%S.%f'), 
         Finish = datetime.datetime.fromtimestamp(end).strftime('%Y-%m-%d %H:%M:%S.%f'), 
         Complete = bytes_read)
      df.append(var) 
      
      d = data[redis_key]
      
      entry = Entry(d["start"], end, d["size"])
      entries.append(entry)
   
   end_times.sort()
   time_series = dict()
   
   diff = end_times[-1] - start_times[0]
   granularity = diff / 100 
   
   t = start_times[0]
   while t < end_times[-1]:
      wss = calculate_working_set_size(t, entries)
      time_series[t] = wss 
      t += granularity
   
   for k,v in time_series.items():
      print("{}, {}".format(k, v))
   
   blue = Color("blue")
   colors = list(blue.range_to(Color("red"), len(sizes)))
   
   print("Creating Gantt chart...")
   fig = ff.create_gantt(df, colors=[c.rgb for c in colors], index_col='Complete', show_colorbar=True, group_tasks = True, showgrid_x=True, showgrid_y=True)
   fig.show()