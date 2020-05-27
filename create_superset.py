import click
import os
from glob import glob
from tqdm import tqdm

DEBUG = False

def id_iterator(fnames):
    i = 0
    for fn in fnames:
        with open(fn) as fin:
            for row in fin:
                try:
                    yield int(row.strip())
                    i+=1
                except ValueError:
                    pass
                if i > 10000 and DEBUG:
                    break
            
        if i > 10000 and DEBUG:
            break

@click.command()
@click.option('--ect', help='path to tweet ids from doi.org/10.14279/depositonce-10012 (ours)')
@click.option('--c19ti', help='path to tweet ids from github.com/echen102/COVID-19-TweetIDs')
@click.option('--cti', help='path to tweet ids from doi.org/10.7910/DVN/LW0BTB')
@click.option('--combined', help='path to unique combined tweet ids dir', required=True)
def main(ect, c19ti, cti, combined):

    if not os.path.isdir(combined):
        print('path to combined datasets does not exist')
        os.makedirs(combined)

    if os.path.isdir(ect):
        fns_ect = [os.path.join(ect, fn) for fn in os.listdir(ect) if fn.endswith(".txt")]
    else:
        print('no path to ect given')

    if os.path.dir(c19ti):
        fns_c19ti = [fn for x in os.walk(c19ti) for fn in glob(os.path.join(x[0], 'coronavirus-tweet-id-*.txt'))]
    else:
        print('no path to c19ti given')

    if os.path.dir(cti):
        fns_cti = [os.path.join(cti,fn) for fn in os.listdir(cti) if fn.endswith(".txt")]
    else:
        print('no path to cti given')

    print(len(fns_ect), ' .txt files found in ect')
    print(len(fns_c19ti), ' .txt files found in c19ti')
    print(len(fns_cti), ' .txt files found in cti')
    
    unique_ids = set()
    total_num_ids = 0
    for new_id in tqdm(id_iterator(fns_ect + fns_c19ti + fns_cti), desc="get unique ids"):
        total_num_ids += 1

        unique_ids.add(new_id)
    
    fname_template = "tweet_ids_{:010d}.txt"
    c_fname = 0
    c_flen = 0
    max_flen = 1000000
    if DEBUG:
        max_flen = 1000

    f = open(os.path.join(combined, fname_template.format(c_fname)), "w")

    for item in tqdm(unique_ids, desc="save ids"):
    
        if c_flen >= max_flen:
            c_fname += 1
            f.close()
            f = open(os.path.join(combined, fname_template.format(c_fname)), "w")
            c_flen = 0
        
        f.write("{}\n".format(item))
        c_flen += 1 
    

if __name__ == "__main__":
    main()