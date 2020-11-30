import argparse 
import sys 
import os 

if __name__ == "__main__":
   parser = argparse.ArgumentParser()
   parser.add_argument("-start", "--starting_value", dest = "starting_value", type=int, help = "Value of the first key.", default = 0)
   parser.add_argument("-end", "--ending_value", dest = "ending_value", type = int, help = "Value of the final key.", default = 1000)
   parser.add_argument("-incr", "--increment", dest = "increment", type = int, help = "Generate the data in partitions of this size.", default = 250)
   parser.add_argument("-file", "--filename", dest = "filename", type = str, help = "The prefix of the filename. The full name will be of the form <PREFIX>part-<x>-<y>-thru-<z>, where x identifies the chunk (chunk 1, chunk 2, etc.), y is the first key in this partition, and z is the last key in the partition.", default = "part")
   parser.add_argument("-threads", "--threads", dest = "threads", type = int, help = "Number of threads to use when generating the data.", default = 1)
   
   args = parser.parse_args()
   starting_val = args.starting_value
   ending_val = args.ending_value
   increment = args.increment 
   filename = args.filename 
   threads = args.threads
   
   filenames = []

   counter = 0
   for i in range(starting_val, ending_val, increment):
      full_filename = "{}part{}-{}-thru-{}".format(filename, counter, i, i + increment)
      command = "gensort -a -b{} {} {}.dat".format(i, increment, full_filename)
      counter = counter + 1
      print("Executing command: {}".format(command))
      os.system(command)
      filenames.append(full_filename)
   
   full_filename = "{}merged-{}-thru-{}".format(filename, 0, ending_val)
   command = "gensort -a -b0 {} {}.dat".format(ending_val, full_filename)
   print("Executing command: {}".format(command))
   os.system(command)   

   print("\n== S3 Keys ==")
   for s3_key in filenames:
      print(s3_key)