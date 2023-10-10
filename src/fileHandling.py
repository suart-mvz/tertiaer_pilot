#import time
#import logging
import os
#from argparse import ArgumentParser
import re
import yaml
#from datetime import datetime


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

