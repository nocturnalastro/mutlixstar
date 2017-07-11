#! /usr/bin/env python
"""
based on the code pvm_xstar using multiprocessing module instead of pvm.

Written by Michael S. Noble (mnoble@space.mit.edu)
Copyright (c) 2008-2009, Massachusetts Institute of Technology

multixstar: Manages parallel execution of multiple XSTAR runs,
using the multiprocessing python module.  XSTAR is part of the LHEASOFT astronomy toolset from
HEASARC, and is used to for calculating the physical conditions and
emission spectra of photoionized gases (Kallman & Bautista 2001).
"""

from __future__ import print_function
from __future__ import division
from __future__ import unicode_literals
from __future__ import absolute_import
import subprocess
import os
import multiprocessing as mp
import getopt
import datetime
import logging


def print_help():
    print ""
    print "multixstar: manages parallel execution of multiple XSTAR jobs, with python's multiprocessing module"
    print "Version 0.1"
    print
    print "Usage:  multixstar [options] <joblist|params>"
    print
    print
    print "Supported options are:"
    print "  -w                the working dir (default will be `./`) WorkDir must exist & be writable"
    # print "  -i <file>         a script to run before running xstar"
    print "  -k                keep log: do not delete after successful run"
    print "  -l <log>          redirect console output to log file"
    print "  -n <N>           set max number processes per host to N (default: 4)"
    # print "  -j  <file|param>  a joblist or a xstinitable parameters"
    print "  -h,--help         prints this message"
    print "  -s,--no-help     surpresses help message so you can run with defaults"
    print
    print "Normally xstinitable will be launched to prompt for XSTAR physical"
    print "parameters and generate a list of XSTAR jobs to run in parallel."
    print "This can be customized by supplying xstinitable parameters on the"
    print "command line (such as mode=h) OR by supplying the name of an "
    print "existing joblist file, in which case xstinitable will not be run"
    print "nor will the generated spectra be collated into a single table"
    print "model with xstar2table."
    print


def run(cmd, env_setup="", stdout=True):
    '''runs cmds in systems shell.'''
    def return_stdout(p):
        return p.communicate()[0]
    if stdout:
        if not env_setup == "":  # if env is set then use it
            return return_stdout(subprocess.Popen(cmd, shell=True, executable=os.getenv("SHELL"), env=env_setup))  # run command with set env => sas hea and ciao running!
        return return_stdout(subprocess.Popen(cmd, shell=True, executable=os.getenv("SHELL"), stdout=subprocess.PIPE))
    else:
        if not env_setup == "":  # if env is set then use it
            return subprocess.Popen(cmd, shell=True, executable=os.getenv("SHELL"), stdout=subprocess.PIPE, env=env_setup, stdin=subprocess.PIPE)  # run command with set env => sas hea and ciao running!
        return subprocess.Popen(cmd, shell=True, executable=os.getenv("SHELL"), stdout=subprocess.PIPE, stdin=subprocess.PIPE)


def run_xstar(xcmd):
    to_return = ""
    os.chdir(xcmd[0])
    to_return + = "Running:" + xcmd[0] + "\n"
    os.environ['PFILES'] = os.getcwd()
    to_return = "copycat" + "\n"
    subprocess.Popen("cp $HEADAS/syspfiles/xstar.par ./", shell=True, executable=os.getenv("SHELL"), stdout=subprocess.PIPE, env=os.environ).wait()
    to_return = xcmd[1] + "\n"
    p = subprocess.Popen("$FTOOLS/bin/" + xcmd[1], shell=True, executable=os.getenv("SHELL"), stdout=subprocess.PIPE, env=os.environ)
    to_return = str(p.pid) + "\n"
    output = p.stdout.readlines()
    os.chdir("../")
    to_return = "\n".join(output) + "\n"
    return to_return


def get_sufix(wdir, extra=""):
    ''' generates a unqie suffix'''
    dirlist = os.listdir(wdir)
    i = 1
    while True:
        if "mxstar." + str(i) + extra in dirlist:
                i += 1
        else:
            break
    return i


def process_flags(argv=None):
    '''
    processing script arguments
    '''
    if argv is None:
        argv = os.sys.argv[1:]

    if len(argv) > 1:
        opts, args = getopt.getopt(argv, "hksl:n:d:", ["help", "no-help"])
        opts = dict(opts)
        if ("-h" in opts.keys()) or ("--help" in opts.keys()):
            print_help()
            if not opts.keys() > 1:
                os.sys.exit()
        else:
            if "-w" in opts.keys():
                workDir = opts["-w"]
            else:
                workDir = "./"

            if "-k" in opts.keys():
                keeplog = True
            else:
                keeplog = False

            if "-l" in opts.keys():
                log_file = opts["-l"]
            else:
                log_file = "mxstar.log"

            if "-n" in opts.keys():
                max_process = int(opts["-n"])
            else:
                max_process = 4
    else:
        ans = "blank"
        while not ans.lower()[0] == "y" and not ans.lower()[0] == "n":
            ans = raw_input("Would you like to continue with defaults?\n").strip() + "blank"
        if ans.lower()[0] == "n":
            print_help()
            os.sys.exit()
        else:
            # set defaults
            max_process = 4
            workDir = "./"
            args = ""
            keeplog = False
            log_file = "mxstar.log"

    return max_process, workDir, args, log_file, keeplog


def check_enviroment(workDir):
    ''' checks heasoft is running
        that workDir exist and is writable
    '''
    # is heasoft running?
    if "FTOOLS" not in os.environ:
        raise OSError("$FTOOLS not set!\n please run heainit and rerun")
    # making new subdir to run in!!!!
    if os.path.isdir(workDir):
        open(workDir + "testing.test", "w")
        os.remove(workDir + "testing.test")
    else:
        raise IOError(workDir + " is not a dir!")


def get_xcmds(args=[], binpath=""):
    binpath += "/"
    if len(args) > 0:
        if not os.path.exists("../" + args[0]):
            to_return = 1 + "\n"
            run(binpath + "xstinitable " + " ".join(args), os.environ)
            joblist = "xstinitable.lis"
        else:
            if not args[0][0] == "/":
                to_return = 2 + "\n"
                joblist = args[0]
                os.rename("../" + joblist, os.getcwd() + "/" + joblist.split("/")[-1])
                if joblist[-4:] == ".fits":
                    old1 = ".fits"
                    new1 = ".lis"
                else:
                    old1 = ".lis"
                    new1 = ".fits"
                os.rename("../" + joblist.replace(old1, new1), os.getcwd() + "/" + joblist.split("/")[-1].replace(old1, new1))
            else:
                os.rename(joblist, workDir + joblist.split("/")[-1])
                joblist = joblist.split("/")[-1]
    else:
        run(binpath + "xstinitable", os.environ)
        joblist = "xstinitable.lis"
    return [x.strip("\n") for x in open(joblist, "r").readlines()]


def make_xcmd_dict(xcmds):
    # pad numbers
    padding = "%0" + str(len(str(len(xcmds)))) + "d"
    xcmd_dict = {}
    for n, x in enumerate(xcmds):
        padded = padding % (n + 1)
        xcmd_dict[padded] = x
    return xcmd_dict


def check_results(padded):
    fault = []
    for p in padded:
        if 'xout_spect1.fits' not in os.listdir(p):
            fault.append(p)
    fault.sort()
    return fault


def main(argv=None):
    # arg processing
    max_process, workDir, args, log_file, keeplog = process_flags()

    check_enviroment(workDir)

    wdir = "mxstar." + str(get_sufix(workDir))
    os.mkdir(wdir)
    os.chdir(wdir)
    if not workDir[-1] == "/":
        workDir + ="/"
    workDir += wdir

    xcmds = get_xcmds(args, os.environ["FTOOLS"] + "/bin/")
    xcmd_dict = make_xcmd_dict(xcmds)
    model_name = dict([z.split("=")for z in xcmd_dict[xcmd_dict.keys()[0]].replace("xstar ", "").split()])["modelname"].replace("'", "").replace('"', '')
    if not os.path.exists(model_name):
        os.mkdir(model_name)
    os.chdir(model_name)

    for pad in xcmd_dict.keys():
        os.mkdir(pad)

    # setup logging
    logFormatter = logging.Formatter("%(message)s")
    rootLogger = logging.getLogger()

    fileHandler = logging.FileHandler(log_file)
    fileHandler.setFormatter(logFormatter)
    rootLogger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler(os.sys.stdout)
    consoleHandler.setFormatter(logFormatter)
    rootLogger.addHandler(consoleHandler)
    rootLogger.level = logging.INFO

    rootLogger.info("Using Dir " + os.getcwd())
    start_time = datetime.datetime.now()
    rootLogger.info("Start time: " + str(start_time))
    p = mp.Pool(processes=max_process)
    runs_return = p.map(run_xstar, xcmd_dict.items(), 1)
    for ret in runs_return:
        rootLogger.info(ret.replace("\n\n", "\n").strip())

    end_time = datetime.datetime.now()
    rootLogger.info("End time: " + str(end_time))

    failed = check_results(xcmd_dict.keys())
    if len(failed) == 0:
        for dest in ['xout_ain.fits', 'xout_aout.fits', 'xout_mtable.fits']:
            run("cp ../xstinitable.fits " + dest)
        padded = xcmd_dict.keys()
        padded.sort()
        for pad in padded:
            run("$FTOOLS/bin/xstar2table xstarspec=./" + pad + "/xout_spect1.fits", os.environ)
        if not keeplog:
            run("rm " + log_file)
    else:
        rootLogger.info("somethings not right in " + ",".join(failed))

if __name__ == '__main__':
    main()
