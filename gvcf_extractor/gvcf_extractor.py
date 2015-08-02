import sys
import os
import shutil
import subprocess
from multiprocessing import Pool
import time
import argparse

def check_file_path(file_list):
    for f in file_list:
        try:
            os.path.isfile(bed_file)
        except OSError:
            print "Can not read %s, Did you specify absolute path to file?"%f
            return False
    
    return True

class Extractor(object):
    def __init__(self, sample_list, bed_file, reference, s3_store, n_tasks):
        #Sample list should be qc.sample_key,vcf location
        self.sample_list = sample_list
        self.bed_file = bed_file
        self.reference = reference
        self.s3_store = s3_store
        self.n_tasks = n_tasks
        self.work_list = []
        self.exec_dir = os.getcwd()

    def execute(self):
        """
        """
        worker_pool = Pool(self.n_tasks)
        the_work_list = self.get_work_list()
        worker_pool.map(worker, self.work_list)
        worker_pool.close()
        worker_pool.join()

    def get_work_list(self):
        """
        """
        with open(self.sample_list, "r") as fh:
            for idx, line in enumerate(fh):
                ls = line.split(",")
                wfid_sid = ls[0]
                genome = ls[1].strip().replace("vcf","genome.vcf.gz")
                fname_vcf_out = "%s.vcf"%(wfid_sid)
                work_dir = os.path.join(self.exec_dir, wfid_sid)
                #If we find an adam file then we continue on to the next file
                if os.path.isfile(os.path.join(work_dir,"%s.adam/_SUCCESS"%wfid_sid)):
                    print "completed %s"%os.path.join(work_dir,"%s.adam/_SUCCESS"%dir_name)
                    continue
                #Here we blow away the directory if it exists
                create_work_dir(work_dir)
                cmd0 = "aws s3 --region us-west-2 cp %s - | pigz -dc | break_blocks --ref %s --region-file %s --exclude-off-target > features.vcf"%(genome, self.reference, self.bed_file)
                commands_to_process = []
                for cmd in [cmd0]:
                    commands_to_process.append(cmd)
                self.work_list.append( (work_dir, commands_to_process, self.exec_dir) )

def worker(work):
    """
    This must be outside of the class due to pickleing of an instance object
    """
    work_dir = work[0]
    commands_to_process = work[1]
    exec_dir = work[2]
    create_work_dir(work_dir)
    os.chdir(work_dir)
    t0 = time.time()
    with open('process.log','w') as f:
        for cmd in commands_to_process:
            p = subprocess.call(cmd, shell=True)
            f.write("%s done in %s\n"%(cmd, time.time() - t0))

        vcf_filename = "features.vcf"
        vcf_filename_new = "mod_features.vcf"
        #massage_vcf(vcf_filename, vcf_filename_new, fname, s3_store, new_name)

        f.write("%s done in %s\n"%("massaging", time.time() - t0))
    os.chdir(exec_dir)

def massage_vcf(vcf_filename, vcf_filename_new, fname, s3_store, new_name):
    '''
    This is only necessary if you want to massage files to adam format. Currently
    adam does not support reference call so you must use the modified version of adam.
    This requires replacing the "." with a "N".  There are other caveats with this approach.
    '''
    with open(vcf_filename) as fh:
        data = fh.readlines()

    with open(vcf_filename_new, 'w') as fh:
        process = False
        for l in data:
            if "#CHROM" in l:
                ls = l.split("\t")
                header = '\t'.join(ls[:9] + [new_name])
                fh.write("##fileformat=VCFv4.1\n")
                fh.write(header+"\n")
                
                process = True
                continue
            
            if process:
                ls = l.split("\t")
                if ls[3] == "n" or ls[3] == "N":
                    pass
                else:
                    if ls[4] == ".":
                        ls[4] = "N"

                    fh.write('\t'.join(ls))
    #Changed this to process files
    #ADAM="/mnt/big_drive/adam"
    ADAM="/mnt/A/adam"
    cmd0 = "%s/bin/adam-submit vcf2adam %s %s.adam"%(ADAM, vcf_filename_new, new_name)
    subprocess.call(cmd0, shell=True)

    cmd1 = "rm %s %s"%(vcf_filename_new, "features.vcf" )
    subprocess.call(cmd1, shell=True)

    #push_to_s3(s3_store, "%s.adam"%new_name)
    #push_to_s3(s3_store, "features.vcf")

def push_to_s3(s3_store, adam_file):
    """
    """
    cmd = "aws s3 sync %s s3://%s/%s"%(adam_file, s3_store, adam_file)
    print "running %s"%cmd
    subprocess.call(cmd, shell=True)

def create_work_dir(w):
    if os.path.isdir(w):
        shutil.rmtree(w)
    os.mkdir(w)


def parse_input():
    parser = argparse.ArgumentParser(description='Pull data from gVCF given coordinates in bed file')
    parser.add_argument('gvcf_file_list', help='S3 location of gVCF')
    parser.add_argument('bed_file', help='File local or in s3 in UCSC bed file format of ranges to pull from gVCF.  \
                        Zero based chrom, start, end(exclusive)')
    parser.add_argument('reference', help='build reference e.g. hg19.fa, hg38.fa')
    parser.add_argument('cores', type=int, help='number of cores to use')
    parser.add_argument('s3_store', help='s3 bucket to drop data to')
    args = parser.parse_args()
    return args

if __name__ == "__main__":
    args = parse_input()

    bed_file = os.path.abspath(args.bed_file)
    gvcf_list = os.path.abspath(args.gvcf_file_list)
    reference = os.path.abspath(args.reference)
    s3_store = args.s3_store

    if check_file_path([bed_file, gvcf_list, reference]):
        tx = time.time()
        worker_pool = Pool(args.cores)
        
        the_work_list = get_work_list(bed_file, gvcf_list, reference, s3_store)
        worker_pool.map(worker, the_work_list)
        worker_pool.close()
        worker_pool.join()
        print time.time() - tx

    cmd_clear_spark_tmp_dir = "rm -r /tmp/spark*"
    subprocess.call(cmd_clear_spark_tmp_dir, shell=True)
