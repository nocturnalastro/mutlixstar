#! /usr/bin/env python3
"""
Based on the code pvm_xstar using multiprocessing module instead of pvm.

Written by Michael S. Noble (mnoble@space.mit.edu)
Copyright (c) 2008-2009, Massachusetts Institute of Technology

multixstar: Manages parallel execution of multiple XSTAR runs,
using the multiprocessing python module.  XSTAR is part of the LHEASOFT astronomy toolset from
HEASARC, and is used to for calculating the physical conditions and
emission spectra of photoionized gases (Kallman & Bautista 2001).
"""

import argparse
import logging
import multiprocessing as mp
import os
import subprocess
import sys
from distutils.dir_util import copy_tree
from pathlib import Path
import psutil
from shutil import copy as copy_file
import re
from datetime import datetime
from time import sleep

__version__ = "0.2.1"


def run(cmd, env=None, shell=None, return_stdout=True, **extra_config):
    """ runs cmds in csh"""
    config = {
        "shell": True,
        "executable": shell or os.getenv("SHELL"),
        "stdout": subprocess.PIPE,
    }
    if env:
        config["env"] = env

    if extra_config:
        config.update(extra_config)
    proc = subprocess.Popen(cmd, **config)
    if return_stdout:
        return proc.communicate()[0]
    return proc


def setup_pfiles():
    dst = Path("pfiles")
    dst.mkdir()
    src = Path(os.getenv("HEADAS")).joinpath("syspfiles/xstar.par")
    copy_file(src, dst)
    os.environ["PFILES"] = str(dst)
    return dst


def get_executeable_dir():
    return Path(os.getenv("FTOOLS")).joinpath("bin")


def get_xstar_output(xstar):
    return map(lambda l: "XSTAR OUTPUT: {}".format(l.decode("utf-8")), xstar.stdout.readlines(),)


def run_xstar(args):
    dir, cmd = args
    result = []
    previous_dir = os.getcwd()
    os.chdir(dir)
    result.append("Running: {}".format(cmd))
    pfiles_dir = setup_pfiles()
    result.append("Copied pfiles to local folder: {}".format(pfiles_dir))
    xstar = run(
        "{exe_dir}/{cmd}".format(exe_dir=get_executeable_dir(), cmd=cmd), return_stdout=False
    )
    result.append("Process ID: {}".format(xstar.pid))
    xstar.wait()
    result.extend(get_xstar_output(xstar))
    os.chdir(previous_dir)
    return "\n".join(result)


def get_new_dir(dir, num, extra):
    return dir.joinpath("mxstar.{}".format("{0}_{1}".format(extra, num) if extra else num))


def make_new_dir(dir, extra=None):
    """ generates a unqie suffix"""
    i = 0
    new_dir = get_new_dir(dir, i, extra)
    while new_dir.exists():
        i += 1
        new_dir = get_new_dir(dir, i, extra)
    new_dir.mkdir()
    return new_dir


def process_flags(argv=None):
    """
    processing script arguments
    """
    usage = "multixstar [options] <joblist|params>"

    description = """
multixstar: manages parallel execution of multiple XSTAR
jobs, with python's multiprocessing module.
Version: {version}
    """.format(
        version=__version__
    )

    epilogue = """
Normally xstinitable will be launched to prompt for XSTAR
physical parameters and generate a list of XSTAR jobs to run in parallel.
This can be customized by supplying xstinitable parameters on the command
line (such as mode=h) OR by supplying the name of an existing joblist
file, in which case xstinitable will not be run nor will the generated
spectra be collated into a single table model with xstar2table
    """
    parser = argparse.ArgumentParser(usage=usage, description=description, epilog=epilogue)
    parser.add_argument(
        "-w",
        "--workdir",
        dest="workdir",
        default="./",
        metavar="WorkDir",
        type=lambda x: Path(x).absolute(),
        help="Work directory to save results of the run",
    )
    parser.add_argument(
        "-k", action="store_true", dest="keeplog", default=False, help="keep log file",
    )
    parser.add_argument(
        "-l",
        "--logfile",
        dest="log_file",
        default="mxstar.log",
        type=lambda x: Path(x).absolute(),
        metavar="LOGFILE",
        help="specify file to save log",
    )
    parser.add_argument(
        "-n",
        "--nproc",
        type=int,
        dest="nproc",
        default=psutil.cpu_count(),
        metavar="NUMPROC",
        help="Max number of processors per host",
    )
    # options stores known arguments and
    # args stores potential xstinitable arguments
    options, args = parser.parse_known_args()
    return options, args


def check_enviroment(dir):
    """Checks heasoft is running and that dir exist and is writable"""

    if "FTOOLS" not in os.environ:
        raise OSError("$FTOOLS not set!\n please run heainit and rerun")

    if not dir.is_dir():
        raise IOError("{} is not a dir".format(dir))

    testfile = dir.joinpath("write_check.test")
    testfile.touch()
    testfile.unlink()


def get_xstar_cmds(args=None, binpath=None):
    print("Making XSTAR commands")
    joblist = None
    if args and len(args) > 0:
        joblist = Path(args[0])
        joblist_local = Path(joblist.name)

        if joblist.exists():
            print("Joblist {} found".format(joblist))
            copy_file(joblist, joblist_local)
            copy_file(joblist.with_suffix(".fits"), joblist_local.with_suffix(".fits"))
            joblist = joblist_local
        elif Path("..").joinpath(joblist).exists():
            joblist = Path("..").joinpath(joblist)
            print("Joblist {} found".format(joblist))
            copy_file(joblist, joblist_local)
            copy_file(joblist.with_suffix(".fits"), joblist_local.with_suffix(".fits"))
        joblist = joblist_local
    else:
        args = []

    if not joblist:
        print("No joblist found: runing xstinitable to make joblist")
        run(
            "{exe} {args}".format(
                exe=binpath.joinpath("xstinitable"), args=" ".join(map(str, args))
            ),
            os.environ,
            stdout=None,
        )
        joblist = Path("xstinitable.lis")
    return joblist.read_text().splitlines()


def make_jobs(cmds):
    print("generating jobs from commands")
    padding = "".join(["%0", str(len(str(len(cmds)))), "d"])
    return {padding % n: x for n, x in enumerate(cmds, start=1)}


def check_results(result_dirs):
    fault = []
    for dir in result_dirs:
        if not dir.joinpath("xout_spect1.fits").exists():
            fault.append(dir)
    return fault


def get_model_name(jobs):
    """Get a job and find the model name"""
    return re.search("modelname='(.*?)'", next(iter(jobs.values()))).group(1)


def make_run_dirs(run_dirs):
    print("Making run dirs:", list(str(d) for d in run_dirs))
    for dir in run_dirs:
        dir.mkdir()


def setup_logging(log_file):
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
    )


def make_xstable(args, run_dirs, model_dir):
    """Build table models from xstar runs"""
    if args:
        base_file = Path(args[0]).with_suffix("fits")
        base_file = Path(base_file.name)  # make sure we get the local version
    else:
        base_file = Path("xstinitable.fits")

    for dest_file in ["xout_ain.fits", "xout_aout.fits", "xout_mtable.fits"]:
        copy_tree(base_file, model_dir.joinpath(dest_file))

    for run_dir in sorted(run_dirs):
        run(
            "{exe} xstarspec={spec}".format(
                exe=get_executeable_dir().joinpath("xstar2table"),
                spec=run_dir.joinpath("xout_spect1.fits"),
            ),
            os.environ,
        )


def process_jobs(pool, jobs, chunksize=1):
    """Run jobs in xstar"""
    logging.info("Using Dir " + os.getcwd())
    start_time = datetime.now()
    logging.info("Start time: {}".format(start_time))

    runs_return = pool.map(run_xstar, jobs.items(), chunksize)

    for ret in runs_return:
        logging.info(ret.strip())

    end_time = datetime.now()
    logging.info("End time: {}".format(end_time))
    logging.info("Duration {}".format(end_time - start_time))


def main(options, args):
    print("Checking enviroment")
    check_enviroment(options.workdir)
    workdir = make_new_dir(options.workdir)

    print("New dir:", workdir)
    os.chdir(workdir)

    print("Getting jobs")
    jobs = make_jobs(get_xstar_cmds(args, get_executeable_dir()))

    model_dir = workdir.joinpath(get_model_name(jobs))
    print("Model dir {}".format(model_dir))

    if not model_dir.exists():
        model_dir.mkdir()

    os.chdir(model_dir)

    run_dirs = [model_dir.joinpath(run_dir) for run_dir in jobs.keys()]
    make_run_dirs(run_dirs)

    # setup logging
    setup_logging(options.log_file)
    print("Starting jobs")
    process_jobs(mp.Pool(processes=options.nproc), jobs)

    failed = check_results(run_dirs)
    if len(failed) > 0:
        logging.info("Somethings not right in {}".format(",".join(map(str, failed))))
        # Exit with non-zero code
        sys.exit(1)
    else:
        make_xstable(args, run_dirs, model_dir)

    if not options.keeplog:
        options.log_file.unlink()


if __name__ == "__main__":
    main(*process_flags())
