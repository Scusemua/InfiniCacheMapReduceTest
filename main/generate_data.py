import argparse 
import sys 
import os 

# This file is a driver script to generate new data/problem sizes for MapReduce tests.
# This assumes you have gensort installed. http://www.ordinal.com/gensort.html
# 
# Example Usage:
# I want to create a 100GB problem size.
# I create a folder called 100gb.
#
# I see that 10,000,000 records are required for 1GB. I need 100x this, so I need to generate
# 1,000,000,000 records (wow!). I need to determine how many chunks/partitions I want the data
# to be in. Let's say I want twenty partitions, where each partition is 5GB. I divide the number
# of records to generate by 20.
#
# 1,000,000,000 / 20 = 50,000,000. This will be the value of the incr parameter.
#
# python3 generate_data.py -start 0 -end 1000000000 -incr 50000000 -file 100gb/100gb -threads 8
#
# You can use however many threads you want. More is usually faster, but I think this can sometimes
# result in errors? I'm not really sure. I usually do 8-16 and it works just fine.
#
# This will generate twenty partitions. The first partition will contain keys 0 to 49,999,999. The
# second will be 50,000,000 to 99,999,999, etc. The partitions generated will be put in the 100gb
# folder that you created. Their names will be:
#
# 100gb-part0-0-thru-50000000.dat
# 100gb-part1-50000000-thru-100000000.dat
# 100gb-part2-100000000-thru-150000000.dat
# 100gb-part3-150000000-thru-200000000.dat
# 100gb-part4-200000000-thru-250000000.dat
# 100gb-part5-250000000-thru-300000000.dat
# 100gb-part6-300000000-thru-350000000.dat
# 100gb-part7-350000000-thru-400000000.dat
# 100gb-part8-400000000-thru-450000000.dat
# 100gb-part9-450000000-thru-500000000.dat
# 100gb-part10-500000000-thru-550000000.dat 
# 100gb-part11-550000000-thru-600000000.dat
# 100gb-part12-600000000-thru-650000000.dat
# 100gb-part13-650000000-thru-700000000.dat
# 100gb-part14-700000000-thru-750000000.dat
# 100gb-part15-750000000-thru-800000000.dat
# 100gb-part16-800000000-thru-850000000.dat
# 100gb-part17-850000000-thru-900000000.dat
# 100gb-part18-900000000-thru-950000000.dat
# 100gb-part19-950000000-thru-1000000000.dat
#
# The reason we want to create them in their own folder is because we can use the AWS CLI to upload
# the whole folder to S3 with a single command.
#
# Enter the folder (e.g., "cd 100gb") and execute the following:
#
# aws s3 sync . <BUCKET_URI>
#
# The bucket I use is infinistore-mapreduce, so I'd execute
#
# aws s3 sync . s3://infinistore-mapreduce
#
# This will automatically upload all of the partitions to S3.
#
# Then, you need to create a file containing the S3 keys of all this data. I generally use the
# naming convention for these files as <SIZE>_S3Keys.txt, but you can use whatever you want in theory.
# So I would make a file in /util called 100GB_S3Keys.txt. This script will output the filenames of all
# the partitions generated. Since we put them into a folder, this would be:
#
# 100gb/100gb-part0-0-thru-50000000.dat
# 100gb/100gb-part1-50000000-thru-100000000.dat
# 100gb/100gb-part2-100000000-thru-150000000.dat
# 100gb/100gb-part3-150000000-thru-200000000.dat
# 100gb/100gb-part4-200000000-thru-250000000.dat
# 100gb/100gb-part5-250000000-thru-300000000.dat
# 100gb/100gb-part6-300000000-thru-350000000.dat
# 100gb/100gb-part7-350000000-thru-400000000.dat
# 100gb/100gb-part8-400000000-thru-450000000.dat
# 100gb/100gb-part9-450000000-thru-500000000.dat
# 100gb/100gb-part10-500000000-thru-550000000.dat 
# 100gb/100gb-part11-550000000-thru-600000000.dat
# 100gb/100gb-part12-600000000-thru-650000000.dat
# 100gb/100gb-part13-650000000-thru-700000000.dat
# 100gb/100gb-part14-700000000-thru-750000000.dat
# 100gb/100gb-part15-750000000-thru-800000000.dat
# 100gb/100gb-part16-800000000-thru-850000000.dat
# 100gb/100gb-part17-850000000-thru-900000000.dat
# 100gb/100gb-part18-900000000-thru-950000000.dat
# 100gb/100gb-part19-950000000-thru-1000000000.dat
#
# I copy-and-paste this into the 100GB_S3Keys.txt file, then use find-and-replace to get rid of the
# beginning. For find, I use "100gb/100gb" and for replace, I use "100gb". This turns the beginning
# of each key from "100gb/100gb-part..." to "100gb-part...". Now you are done!


# The following lists the number of entries required for various problem sizes.
# Use this as a reference when determining how many entries to generate for your desired 
# problem size. 

# 100 records is 10kB.
# 1,000 records is 100kB.
# 10,000 records is 1MB.
# 100,000 records is 10MB.
# 1,000,000 records is 100MB.
# 10,000,000 records is 1GB.
# 100,000,000 records is 10GB.
# 1,000,000,000 records is 100GB.

def get_num_samples(size_bytes):
   return (10000000 * size_bytes) / (1000000000)

if __name__ == "__main__":
   parser = argparse.ArgumentParser()
   parser.add_argument("-start", "--starting_value", dest = "starting_value", type=int, help = "Value of the first key.", default = 0)
   parser.add_argument("-end", "--ending_value", dest = "ending_value", type = int, help = "Value of the final key.", default = 1000)
   parser.add_argument("-incr", "--increment", dest = "increment", type = int, help = "Generate the data in partitions of this size.", default = 250)
   parser.add_argument("-file", "--filename", dest = "filename", type = str, help = "The prefix of the filename. The full name will be of the form <PREFIX>part-<x>-<y>-thru-<z>, where x identifies the chunk (chunk 1, chunk 2, etc.), y is the first key in this partition, and z is the last key in the partition.", default = "part")
   parser.add_argument("-threads", "--threads", dest = "threads", type = int, help = "Number of threads to use when generating the data.", default = 1)
   parser.add_argument("--skip-merged", dest = "skip_merged", action = "store_true", help = "If this flag is passed, then we don't generate the merged data at the end.")
   parser.add_argument("-c", "--calc", dest = "desired_size", type = int, default = -1, help = "Supply a size in bytes; this will print the number of samples needed.")

   args = parser.parse_args()
   starting_val = args.starting_value
   ending_val = args.ending_value
   increment = args.increment 
   filename = args.filename 
   threads = args.threads
   desired_size = args.desired_size 

   if (desired_size > 0):
      print("For {} bytes (i.e., {:.2f} KB, {:.2f} MB, {:.2f} GB), you need {} samples."
         .format(desired_size, desired_size / 1000.0, desired_size / 1000000.0, desired_size / 1000000000.0, get_num_samples(desired_size)))
      exit(0)
   
   filenames = []

   counter = 0
   for i in range(starting_val, ending_val, increment):
      full_filename = "{}part{}-{}-thru-{}".format(filename, counter, i, i + increment)
      command = "gensort -a -b{} {} {}.dat".format(i, increment, full_filename)
      counter = counter + 1
      print("Executing command: {}".format(command))
      os.system(command)
      filenames.append(full_filename)
   
   if not skip_merged:
      full_filename = "{}merged-{}-thru-{}".format(filename, 0, ending_val)
      command = "gensort -a -b0 {} {}.dat".format(ending_val, full_filename)
      print("Executing command: {}".format(command))
      os.system(command)   

   print("\n== S3 Keys ==")
   for s3_key in filenames:
      print(s3_key + ".dat")