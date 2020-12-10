import argparse 
import os
import pandas as pd

MAP_PREFIX = "map_io_data"
REDUCE_PREFIX = "reduce_io_data"
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--prefix", dest = "prefix", type = str, help = "The experiment prefix")
    parser.add_argument("-i", "--io-data-directory", dest = "iodata_dir", default = "iodata", type = str, help = "The iodata/ directory where all the IO data is stored (in respective experiment folders).")
    parser.add_argument("-j", "--job", dest = "job", default = None, type = str, help = "Name of the job for which you want to process IORecords. Ignores records not of this job type.")
    args = parser.parse_args()

    root_dir = os.path.join(args.iodata_dir, args.prefix)

    for root, subdirs, files in os.walk(root_dir):
        print(files)
        for file in files:
            if args.job:
                if not args.job in file:
                    continue         
            full_filepath = os.path.join(root, file)
            print("Processing file \"%s\"" % full_filepath)
            df = pd.read_csv(full_filepath, sep = " ", header = None, names = ["TaskNumber", "TaskKey", "SizeBytes", "StartTime", "EndTime"])
            print(df)