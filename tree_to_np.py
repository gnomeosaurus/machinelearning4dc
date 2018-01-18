from __future__ import print_function
import ROOT as rt
import root_numpy
import pickle
import argparse
import os
import sys
import shutil
import numpy as np
import logging


"""
python tree_to_np.py [files and/or directories to be converted] 
  -d [destination of resulting numpy arrays] 
  -n [path to the connected np array]
  -c [True/False clean (remove) the directory of concatenate files]
python tree_to_np.py source1 -d ./ -name_conn conn.pkl -clean True
"""

def convert_tree_to_np(sources, destination, npy_files=[]):
    """ Converts the root files in sources to numpy arrays
    Params
        sources : list
            root file paths/names or path to directory of root files
        destination : str
            path to directory where the npy files will be saved
        npy_files : list
            list of already converted files (for recursive functionality)
    Returns
        list
            paths to the converted files 
    """
    for i in xrange(len(sources)):
        if os.path.isdir(sources[i]) and ('failed' not in sources[i]):
            # source is a directory -> recurse on all files in directory
            new_sources = [sources[i]+'/'+e for e in os.listdir(sources[i])]
            new_destination = destination+'/'+sources[i].split('/')[-1]

            print('new_sources ', len(new_sources), new_sources[-9:])
            print('new_destination ', new_destination)
            logging.info('new_sources '+ str(len(new_sources))+' '+ str(new_sources[-9:]))
            logging.info('new_destination '+ new_destination)
            os.mkdir(new_destination)
            convert_tree_to_np(new_sources, new_destination, npy_files)
        else:
            if ".root" in sources[i]:
                try:
                    # print(i, sources[i])
                    logging.info(str(i) +' '+ sources[i])
                    print(str(i)+' ', end="")
                    sys.stdout.flush()

                    tChain = rt.TChain('MyAnalysis/MyTree')
                    tChain.Add(sources[i])

                    array = root_numpy.tree2array(tChain)
                    # print 'Total number of entries: ',tChain.GetEntries()
                    
                    pkl_file_name = destination+'/'+sources[i].split('/')[-1][:-5]
                    np.save(pkl_file_name, array)
                    npy_files.append(pkl_file_name+'.npy')
                except Exception as e:
                    if os.path.exists(failed.pkl):
                        continue
                    else:
                        mylist=[]
                        with open('failed.pkl', 'wb') as f:
                            pickle.dump(mylist, f)
                    print("")
                    print(e)
                    print(sources[i], " ** FAILED ** ")
                    logging.error(sources[i]+ " ** FAILED ** ")
                    logging.error(e)

                    f = open('failed.pkl', 'rb')
                    failed = pickle.load(f)
                    f.close()
                    failed.append(sources[i])
                    f = open('failed.pkl', 'wb')
                    pickle.dump(failed, f)
                    f.close()
                        
                

    return npy_files

def concatenate_pickles(npy_files, name_conn, clean=False, sources=[], destination="./"):
    """Concatenate numpy arrays in multiple files into one numpy array and saves it.
    Params
        npy_files : list
            paths to np arrays
        name_conn : str
            path to the concatenated np array (destination)
    Returns
        void
    """
    # loading the first array
    array = np.load(npy_files[0])
    for npy_file_name in npy_files[1:]:
        print(npy_file_name)
        next_array = np.load(npy_file_name)
        
        array = np.concatenate((array, next_array), axis=0)

    np.save(name_conn, array)

    if clean:
        # removes the directory of unconcatenated npy files
        for i in xrange(len(sources)):
            if os.path.isdir(sources[i]):
                shutil.rmtree(destination+'/'+sources[i].split('/')[-1])


def main():
    logging.basicConfig(filename='tree_to_np.log',level=logging.DEBUG, format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    datefmt='%m-%d %H:%M')
    parser = argparse.ArgumentParser(prog='PROG', usage='%(prog)s [options]')
    parser.add_argument( dest='sources', metavar='N', type=str, nargs='+', help='Help sources') #'-s', '--sources',
    parser.add_argument("-d", "--destination", dest="destination", default="./", help="Help destination")
    parser.add_argument("-n", "-name_conn", dest="name_conn", default="", help="Help name_conn")
    parser.add_argument("-c", "-clean", dest="clean", default=False, help="Help clean")
    args = parser.parse_args()

    print(args)
    sources = args.sources
    destination = args.destination
    name_conn = args.name_conn
    clean = not( args.clean=='False' )

    npy_files = convert_tree_to_np(sources, destination)
    print(npy_files)
    logging.info(str(npy_files))

    if name_conn:
        concatenate_pickles(npy_files, name_conn, clean=clean, sources=sources, destination=destination)


if __name__ == '__main__':
    main()
    print("tree_to_np DONE")


