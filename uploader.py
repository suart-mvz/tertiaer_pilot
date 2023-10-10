#import time
import logging
import os
#import subprocess
from argparse import ArgumentParser
#import re
#import yaml
from datetime import datetime

from src import fileHandling
from src import uploadFunctions

# setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# format, filehandling
formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(funcName)s : %(message)s") # adapt
filepath = os.path.dirname(os.path.abspath(__file__))

today = datetime.today().strftime('%Y-%m-%d')
filepath = os.path.join(filepath, f"upload_{today}.log") # adapt
file_handler = logging.FileHandler(filepath)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
#logging.basicConfig(level=logging.INFO, format=formatter, filename=filepath)


def cli():
    parser = ArgumentParser()
    parser.add_argument("--experiment", "-e",
                         help="which type of experiment to test. Values can be wes/wgs",
                         default="wes", dest="experiment"
                       )
    parser.add_argument("--datatypes", "-d",
                         help="list of data types to upload; e.g. vcf,bam,cram",
                         dest="dataType", required=True
                       )
    parser.add_argument("--internal", "-i",
                         help="whether to upload internal samples - ONLY IF CONTRACT IS AVAILABLE!",
                         dest="internal", action="store_true"
                       )
    parser.add_argument("--s3",
                         help="whether to upload to cloud storage",
                         action="store_true", dest="s3"
                       )
    parser.add_argument("--provider", "-p",
                         help="which software provider to test; e.g genoox/illumina",
                         dest="provider"
                       )
    parser.add_argument("--count", "-c",
                         help="how many smaples to upload",
                         default="1", dest="sampleCount"
                       )
    args = parser.parse_args()
    return args.experiment, args.dataType, args.internal, args.s3, args.provider, args.sampleCount


def main():
    """reads config,
       checks if upload to provider is allowed
       selects files and uploads them
       stores already uploaded files
    """    
    # experiment: wes/wgs...
    # data: public/internal
    # filetypes: bam,snv-vcf,cnv-vcf,sv-vcf...
    # check if software should be able to recognize this file
    # contract check
    # upload to cloud storage or automated case creation?
    # s3 profile: genoox/illumina
    # how many to upload (config file?)
    exp, dtype, internal, s3, provider, sampleCount = cli()
    #TEST####
    conf = fileHandling.read_config()
    internal = False
    provider = "genoox"
    experiment = "wes"
    count = 10
    fileTypes = "vcf,bam"
    s3 = True
    #END TEST#
    if internal:
        if not conf["providers"][provider]["contract"] == "yes":
            logger.error("upload of internal files not allowed for {}, contracts pending.".format(provider))
            exit()

    samples = conf["samples"][experiment]
    uploadedFile = os.path.join("/mnt/data/provider/", provider, "uploaded.txt") # conf["providers"][provider]["uploaded"]
    with open(uploadedFile, "r") as freader:
        uploaded = freader.readlines()
        uploaded = [u.strip() for u in uploaded]
    if s3:
        bucket = conf["providers"][provider]["bucket"]
    samples = [s for s in samples if s not in uploaded]
    samples = samples[:count]
    if len(samples) < count:
        logger.info("not enough samples to upload; available samples: {}, requested samples: {}".format(len(samples), count))
    print(samples)
    fileTypes = fileTypes.split(",")
    allSampleFiles = []
    for sample in samples:
        sampleFiles = []
        fastq, bam, cram, snvVcf, dragenCnvVcf, svVcf, bamIndex, cramIndex, snvIndex, dragenCnvIndex, svIndex = fileHandling.get_files(sampleID = sample)
        if "fastq" in fileTypes:
            if not fastq:
                logger.critical("fastq file not found for sample {} but was required".format(sample))
                exit()
            sampleFiles.append(fastq)
        if "bam" in fileTypes:
            if not bam:
                logger.critical("bam file not found for sample {} but was required".format(sample))
                break
            sampleFiles.append(bam)
            sampleFiles.append(bamIndex)
        if "cram" in fileTypes:
            if not cram:
                logger.critical("cram file not found for sample {} but was required".format(sample))
                break
            sampleFiles.append(cram)
            sampleFiles.append(cramIndex)
        if "vcf" in fileTypes:
            if not snvVcf:
                logger.critical("vcf file not found for sample {} but was required".format(sample))
                break
            sampleFiles.append(snvVcf)
            sampleFiles.append(dragenCnvVcf)
            sampleFiles.append(svVcf)
        allSampleFiles.append(sampleFiles)
    print(allSampleFiles)

    #uploadToS3(provider, bucket, sampleFiles, logger)
    fileHandling.write_uploaded(samples, provider)


if __name__ == "__main__":
    main()

