#!/usr/bin/env python


import argparse
import os
import sys
import subprocess
import re
import glob
import datetime




# updated March 4, 2019 (DG) for new location, added log file, don't keep copy other than
#   that in pacbio-aspera directory, and merge bam files.
# David Gordon, July 25, 2018 with design by Katy Munson
# To be run from aspera directory

# In the current directory, there must be a file manifest.tab
# This file looks like this:
#
#sample_name run_id cell_id CCS_or_HGAP jobID multiplex_sample_name

# it is ok to have or not have a header.  If you have a header, it must
# start with #sample_name


szLogFile = "/net/eichler/vol26/projects/sequencing/pacbio/backups/cost_center_log/log.txt"



# Aspera directory structure:
# sample name
#    tar balls
#    ccs_jobid (subdir)
#    hgap_jobid (subdir)
#    multiplex-sampleid (subdir)
#       ccs_jobid (subdir)
#       hgap_jobid (subdir)
#    multiplex-sampleid (subdir)
#       ccs_jobid (subdir)
#       hgap_jobid (subdir)



parser = argparse.ArgumentParser()
#parser.add_argument("--manifest", required = True )
#parser.add_argument("--aspera", required = True )
args = parser.parse_args()


# should be running in an aspera directory
# which looks like:
# /net/eichler/vol26/projects/sequencing/pacbio/pacbio-aspera19

szInitialDirectory = os.getcwd()

szPattern = "/net/eichler/vol26/projects/sequencing/pacbio/cost_center/pacbio-aspera"

if ( not szInitialDirectory.startswith( szPattern ) ):
    sys.exit( "the current directory is " + szInitialDirectory + " but should be a path starting with " + szPattern )

szAsperaNumber = re.sub( szPattern, "", szInitialDirectory )
if ( not szAsperaNumber.isdigit() ):
    sys.exit( "the current directory is " + szInitialDirectory + " but should be a path starting with " + szPattern + " and ending with a number but instead it ends with " + szAsperaNumber  )

szManifest = "manifest.tab"


if ( not os.path.exists( szManifest ) ):
    sys.exit( "in the current directory should be a file " + szManifest + " but it is missing" )

aLines = []
with open( szManifest, "r" ) as fManifest:
    while True:
        szLine = fManifest.readline()
        if ( szLine == "" ):
            break
        aLines.append( szLine.rstrip() )

# columns of the manifest:

#sample_name run_id cell_id CCS_or_HGAP jobID multiplex_sample_name

if ( aLines[0].startswith( "#sample_name" ) ):
    nStartIndex = 1
else:
    nStartIndex = 0

for n in range( nStartIndex, len( aLines ) ):
    aWords = aLines[n].split("\t")


    if ( len( aWords ) < 3 ):
        sys.exit( "line: " + aLines[n] + " didn't even have the sample name and runID and cellID on the line" )

    szSample = aWords[0]
    szRunID = aWords[1]
    szCellID = aWords[2]


    # added Apr 4, 2019 to handle case in which there are extra tabs inserted in the manifest

    print "szSample = " + szSample + " szRunID = " + szRunID + " szCellID = " + szCellID

    if ( szRunID == "" ):
        sys.exit( "the run id is empty on this line of the manifest:\n" + aLines[n] + "\nThis might be due to extra tabs on this line" )

    if ( szCellID == "" ):
        sys.exit( "the cell id is empty on this line of the manifest:\n" + aLines[n] + "\nThis might be due to extra tabs on this line" )


    if ( len( aWords ) >= 4 ):
        szCCSorHGAP = aWords[3].upper()
    else:
        szCCSorHGAP = ""

    if ( len( aWords ) >= 5 ):
        szJobID = aWords[4]
    else:
        szJobID = ""

    if ( len( aWords ) >= 6 ):
        szMultiplexSampleName = aWords[5]
        if ( " " in szMultiplexSampleName ):
            sys.exit( "please do not put spaces in sample names--there is a space in " + szMultiplexSampleName + " in " + aLines[n] )
    else:
        szMultiplexSampleName = ""


    # create sample directory in aspera directory

    os.chdir( szInitialDirectory )

    szCommand = "mkdir -p " + szSample
    print "about to execute: " + szCommand
    subprocess.check_call( szCommand, shell = True )

    # szCommand = "chgrp pacbio-aspera " + szSample
    # print "about to execute: " + szCommand
    # subprocess.check_call( szCommand, shell = True )



    os.chdir( szSample )

    szAsperaSampleDir = os.getcwd()


    szRawDataPath = "/net/eichler/vol24/projects/sequencing/pacbio/backups/smrtlink_data/" + szRunID + "/" + szCellID

    if ( not os.path.exists( szRawDataPath ) ):
        sys.exit( szRawDataPath + " doesn't exist, but it should if the run ID is really szRunID as indicated by this line in the manifest: " + aLines[n] )


    szTarBall = szRunID + "_" + szCellID + ".tar.gz"
    szTarBallFullPath = szAsperaSampleDir + "/" + szTarBall

    if ( not os.path.exists( szTarBall ) ):
        # must create tar ball and copy it

        szCommand = "set -o pipefail && cd " + szRawDataPath + " && tar -cvf - * | gzip -c >" + szTarBallFullPath
        print "about to execute: " + szCommand
        subprocess.check_call( szCommand, shell = True )

        # szCommand = "chgrp pacbio-aspera " + szTarBall
        # print "about to execute: " + szCommand
        # subprocess.check_call( szCommand, shell = True )

        szCommand = "md5sum " +  szTarBall + " >" + szTarBall + ".md5"
        print "about to execute: " + szCommand
        subprocess.check_call( szCommand, shell = True )

    else:
        # Melanie prefers this *not* be a fatal error--she wants the script to
        # keep running.  This would occur when additional analysis is performed/distributed.
        print szTarBall + " exists so no need to create it again or copy to the aspera directory"

    if ( szJobID != "" ):
        # must copy analysis files to aspera directory
        # per Katy ( July 2018), analysis files are NOT to be saved to
        # /net/eichler/pacbio/data.  They can be recreated if necessary.

        # changed from 000 to 00* per Melanie Sep 2018 since the smrt analysis
        # software will sometimes use 001 instead of 000
        szSmrtLinkRoot = "/net/eichler/vol24/projects/sequencing/pacbio/smrt-link/userdata/jobs_root/00*/"

        szSmrtLinkDir = szSmrtLinkRoot + szJobID

        aTestArray = glob.glob( szSmrtLinkDir )
        
        if ( len( aTestArray ) == 0  ):
            sys.exit( "the analysis files should be in " + szSmrtLinkDir + " but this directory didn't exist" )

        # if reached here, analysis directory exists


        # is this a multiplexed sample?

        if ( szMultiplexSampleName != "" ):
            # this is a multiplexed sample so add the multiplex sample id and cd into it

            szCommand = "mkdir -p " + szMultiplexSampleName
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )

            szCommand = "chgrp pacbio-aspera " + szMultiplexSampleName
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )

            
            os.chdir( szMultiplexSampleName )



        if ( szCCSorHGAP == "CCS" ):

            szJobIDDir = "CCS_" + szJobID
            szCommand = "mkdir -p " + szJobIDDir
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )

            # szCommand = "chgrp pacbio-aspera " + szJobIDDir
            # print "about to execute: " + szCommand
            # subprocess.check_call( szCommand, shell = True )


            os.chdir( szJobIDDir )

            szFullPathToCopy=szSmrtLinkDir + "/tasks/pbcoretools.tasks.gather_ccsset-1/file.consensusreadset.xml"
            szCommand = "cp -v " + szFullPathToCopy + " ."
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )
            
            # the "*" is necessary in some cases since smrtlink can
            # add additional text if it thinks there is a barcode
            
            szFullPathToCopy = szSmrtLinkDir + "/tasks/pbcoretools.tasks.bam2fasta_ccs-0/ccs*.fasta*"
            szCommand = "cp -v " + szFullPathToCopy + " ."
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )

            szFullPathToCopy = szSmrtLinkDir + "/tasks/pbcoretools.tasks.bam2fastq_ccs-0/ccs*.fastq*"
            szCommand = "cp -v " + szFullPathToCopy + " ."
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )

            szFullPathToCopy = szSmrtLinkDir + "/tasks/pbreports.tasks.ccs_report-0/ccs_report.json"
            szCommand = "cp -v " + szFullPathToCopy + " ."
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )

            # szCommand = "chgrp pacbio-aspera *"
            # print "about to execute: " + szCommand
            # subprocess.check_call( szCommand, shell = True )

            # added March 4, 2019 to add bam files, then
            # modified on March 14, 2019 to handle case of ccs.bam
            # already existing

            if ( os.path.exists( "ccs.bam" ) ):
                print "ccs.bam already exists so not recreating it"
            else:
                szCommand = "find " + szSmrtLinkDir + " -name \"ccs.bam\" -exec samtools merge ccs.bam {} +"
                print "about to execute: " + szCommand
                subprocess.check_call( szCommand, shell = True )
            
             
        elif( szCCSorHGAP == "HGAP" ):
            
            szJobIDDir = "HGAP_" + szJobID
            szCommand = "mkdir -p " + szJobIDDir
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )


            # szCommand = "chgrp pacbio-aspera " + szJobIDDir
            # print "about to execute: " + szCommand
            # subprocess.check_call( szCommand, shell = True )


            szFullPathToCopy= szSmrtLinkDir + "/tasks/pbcoretools.tasks.gather_contigset-1/file.contigset.fasta"
            szCommand = "cp -v " + szFullPathToCopy + " ."
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )

            szFullPathToCopy= szSmrtLinkDir + "/tasks/pbcoretools.tasks.gather_contigset-1/file.contigset.fasta.fai"
            szCommand = "cp -v " + szFullPathToCopy + " ."
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )

            szFullPathToCopy= szSmrtLinkDir + "/tasks/pbcoretools.tasks.gather_contigset-1/file.contigset.xml"
            szCommand = "cp -v " + szFullPathToCopy + " ."
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )

            szFullPathToCopy= szSmrtLinkDir + "/tasks/pbcoretools.tasks.gather_fastq-1/file.fastq"
            szCommand = "cp -v " + szFullPathToCopy + " ."
            print "about to execute: " + szCommand
            subprocess.check_call( szCommand, shell = True )

            # szCommand = "chgrp pacbio-aspera *"
            # print "about to execute: " + szCommand
            # subprocess.check_call( szCommand, shell = True )


        else:
            sys.exit( "line " + aLines[n] + " has the CCSorHGAP as " + szCCSorHGAP + " but it should be either CCS or HGAP" )

    # now make the aspera data so that others in the lab can delete it

    szCommand = "chmod -R g+wx " + szAsperaSampleDir
    print "about to execute: " + szCommand
    subprocess.check_call( szCommand, shell = True )


# logging information for the purpose of automatic deletion later

# how big is the data

szCommand = "du -s *"
print "about to execute: " + szCommand
szOutput = subprocess.check_output( szCommand, shell = True )

aWords = szOutput.split()
# looks like:
# > du -s .
# 32      .

nSize = int( aWords[0] )

today = datetime.datetime.today()

szFormat = "%Y-%m-%d %H:%M:%S"
szToday = today.strftime( szFormat )

szToPrint = "created: " + szToday + " size (kb): " + str( nSize ) + " " + szInitialDirectory

os.chdir( szInitialDirectory )




# append since the user might run this multiple times on the same pacbio-aspera directory
with open( "CREATED", "a" ) as fCreated:
    fCreated.write( szToPrint + "\n" )


with open( szLogFile, "a" ) as fLogFile:
    fLogFile.write( szToPrint + "\n" )
            

# allows Katy/Melanie to overwrite/delete each others'
szCommand = "chgrp -R pacbio-aspera *"
print "about to execute: " + szCommand
subprocess.check_call( szCommand, shell = True )



print "Have a nice day!"
