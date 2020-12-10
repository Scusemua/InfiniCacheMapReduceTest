import argparse 
import fileinput
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
    
    srt_map = open(os.path.join(root_dir, "srt_map.csv"), "w")
    srt_reduce = open(os.path.join(root_dir, "srt_reduce.csv"), "w")
    wc_map = open(os.path.join(root_dir, "wc_map.csv"), "w")
    wc_reduce = open(os.path.join(root_dir, "wc_reduce.csv"), "w")    

    for root, subdirs, files in os.walk(root_dir):
        print(files)
        for file in files:
            if args.job:
                if not"wc" in file and not "srt" in file:
                    continue
                if file.endswith(".bak"):
                    continue
            full_filepath = os.path.join(root, file)
            # # Read in the file
            # with open(full_filepath, 'r') as _file :
            #     filedata = _file.read()

            # # Replace the target string
            #     filedata = filedata.replace('{', '')
            #     filedata = filedata.replace('}', '')

            # # Write the file out again
            # with open(full_filepath, 'w') as _file:
            #     _file.write(filedata)
            print("Processing file \"%s\"" % full_filepath)
            with open(full_filepath, 'r') as _file:
                filedata = _file.read()
                if "srt" in full_filepath:
                    if "map" in full_filepath:
                        srt_map.write(filedata)
                    elif "reduce" in full_filepath:
                        srt_reduce.write(filedata)
                    else:
                        print("Unknown file type: " + file)
                elif "wc" in full_filepath:
                    if "map" in full_filepath:
                        wc_map.write(filedata)
                    elif "reduce" in full_filepath:
                        wc_reduce.write(filedata)
                    else:
                        print("Unknown file type: " + file)
                else:
                    print("Unknown job: " + file)
            #df = pd.read_csv(full_filepath, delim_whitespace=True, header = None, names = ["TaskNumber", "TaskKey", "SizeBytes", "StartTime", "EndTime"])
            #print(df)