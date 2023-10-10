import time
import logging
import os
import subprocess
from argparse import ArgumentParser
import re
import yaml
from datetime import datetime
import zipfile


def uploadToS3(profile, bucket, sampleList, experiment, logger):
    """_summary_

    Parameters
    ----------
    profile : str
        which aws profile to use
    bucket : str
        bucket ID
    sampleList : list
        list with sampleIDs
    experiment : str
        WES/WGS...
    logger : logging object
    """    
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


def uploadToQiagen(sampleFiles, logger):
    # zip files
    zipTime = datetime.now()
    for sample in sampleFiles:
        sampleName = re.search("([A-Z0-9]+\-.*?)\.", sample[0]).group(1)
        logger.info("starting to zip {}".format(sampleName))
        with zipfile.ZipFile("{}.zip".format(sampleName), mode="w") as zipArchive:
            for file in sample:
                if re.search("vcf", file):
                    zipArchive.write(file)
        logger.info("done zipping {}; took {}".format(sampleName, datetime.now()-zipTime))
        uploadTime = datetime.now()
        #upload

