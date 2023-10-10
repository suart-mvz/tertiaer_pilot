import time
import logging
import os
from argparse import ArgumentParser
import re
import yaml
from datetime import datetime

# setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# format, filehandling
formatter = logging.Formatter("%(asctime)s : %(levelname)s : %(funcName)s : %(message)s") # adapt
filepath = os.path.dirname(os.path.abspath(__file__))

today = datetime.today()
filepath = os.path.join(filepath, f"upload_{today}.log") # adapt
file_handler = logging.FileHandler(filepath)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
#logging.basicConfig(level=logging.INFO, format=formatter, filename=filepath)

def uploadToS3(profile, bucket, sampleList, experiment):
    # test upload to S3 bucket
    # check individual time for each file and sample as well as the file sizes that were uploaded
    logger.info("starting to upload")
    startTime = datetime.now()
    fileCount = 0
    sampleCount = 0
    totalSize = 0
    for sampleFiles in sampleList:
        for uploadFile in sampleFiles:
            fileSize = os.path.getsize(uploadFile)
            logger.info("start uploading {}".format(uploadFile))
            uploadStart = datetime.now()
            if profile == "genoox":
                # genoox prefers to have folders per assay (wes/wgs), however there are no folders in aws since it is an object store
                targetFolder = "/{}/".format(experiment)
                command = "aws s3 cp {} {}{} --profile {}".format(uploadFile, bucket, targetFolder, profile)
            else:
                command = "aws s3 cp {} {} --profile {}".format(uploadFile, bucket, profile)
            subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            std_out, std_err = pipe.communitcate()
            uploadEnd = datetime.now()
            uploadTime = uploadEnd - uploadStart
            uploadTime = uploadTime.total_seconds()
            if pipes.returncode != 0:
                logger.critical(std_err.strip())
            elif len(std_err):
                logger.warning(std_err.strip())
            else:
                fileCount += 1
                totalSize += fileSize
                logger.info("uploaded {} with size {}. Duration {}".format(uploadFile, fileSize, uploadTime))
        sampleCount += 1
    endTime = datetime.now()
    uploadTime = endTime - startTime
    uploadTime = uploadTime.total_seconds()
    logger.info("done uploading files. Duration: {}".format(uploadTime))
    logger.info("uploaded {} files".format(fileCount))
    logger.info("uploaded {} samples".format(sampleCount))
    logger.info("uploaded {} bytes in total".format(totalSize))


def get_files(sampleID, dataFolder= "public", experiment = "wes"):
    fastq = bam = cram = snvVcf = dragenCnvVcf = svVcf = ""
    bamIndex = cramIndex = snvIndex = dragenCnvIndex= svIndex = ""
    if experiment == "wes":
        cnvSearchString = "downsampled"
    else:
        cnvSearchString = "cnv"
    for root, dirs, files in os.walk(os.path.join("/mnt/data/test_data", dataFolder)):
        if re.search(experiment, root):
            for file in files:
                if re.search(sampleID, file):
                    if file.endswith("vcf.gz"):
                        if re.search(cnvSearchString, file):
                            dragenCnvVcf = os.path.join(root, file)
                        elif re.search("hard-filtered.vcf", file):
                            snvVcf = os.path.join(root, file)
                        elif re.search("sv", file):
                            svVcf = os.path.join(root, file)
                    if file.endswith("vcf.gz.tbi"):
                        if re.search(cnvSearchString, file):
                            dragenCnvIndex = os.path.join(root, file)
                        elif re.search("hard-filtered", file):
                            snvIndex = os.path.join(root, file)
                        elif re.search("sv", file):
                            svIndex = os.path.join(root, file)
                    elif file.endswith("bam"):
                        bam = os.path.join(root, file)
                    elif file.endswith("bam.bai"):
                        bamIndex = os.path.join(root, file)
                    elif file.endswith("cram"):
                        cram = os.path.join(root, file)
                    elif file.endswith("cram.crai"):
                        cramIndex = os.path.join(root, file)
                    elif file.endswith("fastq.gz"):
                        fastq = os.path.join(root, file)
    return fastq, bam, cram, snvVcf, dragenCnvVcf, svVcf, bamIndex, cramIndex, snvIndex, dragenCnvIndex, svIndex


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


def read_config():
    with open("/mnt/data/test_data/config.yaml", "r") as yamlFile:
        configFile = yaml.safe_load(yamlFile)
    #print(configFile["providers"].keys())#
    return configFile


def write_uploaded(sampleList, provider):
    # to store which files were already uploaded
    uploadedFile = os.path.join("/mnt/data/provider/", provider, "uploaded.txt") # conf["providers"][provider]["uploaded"]
    with open(uploadedFile, "a") as fwriter:
        for sample in sampleList:
            fwriter.write("{}\n".format(sample))


def main():
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
    conf = read_config()
    internal = False
    provider = "genoox"
    experiment = "wgs"
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
        fastq, bam, cram, snvVcf, dragenCnvVcf, svVcf, bamIndex, cramIndex, snvIndex, dragenCnvIndex, svIndex = get_files(sampleID = sample)
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
            if not vcf:
                logger.critical("vcf file not found for sample {} but was required".format(sample))
                break
            sampleFiles.append(snvVcf)
            sampleFiles.append(dragenCnvVcf)
            sampleFiles.append(svVcf)
        allSampleFiles.append(sampleFiles)
    print(sampleFiles)
    
    #uploadToS3(provider, bucket, sampleFiles)
    write_uploaded(samples, provider)


if __name__ == "__main__":
    main()

