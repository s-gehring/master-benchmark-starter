import itertools
import collections
from typing import Any, Tuple
import subprocess, os
import datetime

global_ratio = 0.02

options_for_all = collections.OrderedDict()

options_for_all["Machines"] = [6, 12]
options_for_all["MemoryInGB"] = [10]
options_for_all["Cores"] = [1]
options_for_all["SizeInKB"] = [(1, 193), (193, 459), (459, 10000)]
options_for_all["PipelineId"] = [0, 1, 2]

spark_options = {
    "Serialization": [
        "gehring.uima.distributed.serialization.UimaSerialization"
        #"gehring.uima.distributed.serialization.XmiCasSerialization"
    ],
    "Compression": [
        "gehring.uima.distributed.compression.NoCompression"
        #"gehring.uima.distributed.compression.ZLib"
    ]
}

spark_count = 0


def sparkle(machines, memory, cores, size, serializer, compression, pipeline, ratio=1.0):
    print("Sparkling with " + str(machines) + " machines, each having " + str(cores) + " cores and " + str(
        memory) + "GB memory.")
    print("Using '" + serializer + "' and '" + compression + "'.")
    print("Only looking at files between " + str(size[0]) + "KB and " + str(size[1]) + "KB.")
    log_directory = "/home/simon.gehring/master-logs/spark_" + str(spark_count) + "/"
    os.chdir("/home/simon.gehring/git/master-thesis-spark")
    global spark_count
    spark_count += 1
    current_environment = os.environ.copy()
    current_environment.update({
        "SUP_GUTENBERG_RATIO": str(ratio),
        "SUP_COMPRESSION_ALGORITHM": compression,
        "SUP_SERIALIZER": serializer,
        "FILESIZE_MIN": str(size[0] * 1024),
        "FILESIZE_MAX": str(size[1] * 1024),
        "SUP_LOG_FILES": log_directory,
        "MEMORY_PER_MACHINE_IN_GB": str(memory),
        "CORES_PER_MACHINE": str(cores),
        "DOCUMENT_DIR": "/home/simon.gehring/git/master-thesis-spark/documents",
        "SUP_SINGLE_INSTANCE": "",
        "SUP_PIPELINE_ID": str(pipeline)
    })
    command = [
        "docker-compose",
        "-f", "compose-1m2s.yaml",
        "up",
        "--scale", "slave-two=" + str(machines),
        "--exit-code-from", "submitter"
        # "-d"
    ]
    try:
        result = subprocess.check_output(" ".join(command), shell=True, env=current_environment)
        result = result.decode('unicode-escape')
        f = open('/home/simon.gehring/master-logs/submitter_spark_' + str(spark_count) + '.log', 'w')
        f.write(result)
        f.close()
    except subprocess.CalledProcessError as e:
        print(e.output.decode('unicode-escape'))


uima_count = 0


def uimale(machines, memory, cores, size, pipeline, ratio=1.0):
    print("UIMA AS-ing with " + str(machines) + " machines, each having " + str(cores) + " cores and " + str(
        memory) + "GB memory.")
    print("Only looking at files between " + str(size[0]) + "KB and " + str(size[1]) + "KB.")
    log_directory = "/home/simon.gehring/master-logs/uima_" + str(uima_count) + "/"
    os.chdir("/home/simon.gehring/git/master-thesis-uimaas")
    global uima_count
    uima_count += 1
    current_environment = os.environ.copy()
    current_environment.update({
        "SUP_GUTENBERG_RATIO": str(ratio),
        "FILESIZE_MIN": str(size[0] * 1024),
        "FILESIZE_MAX": str(size[1] * 1024),
        "SUP_LOG_FILES": log_directory,
        "MEMORY_PER_MACHINE_IN_GB": str(memory),
        "CORES_PER_MACHINE": str(cores),
        "DOCUMENT_DIR": "/home/simon.gehring/git/master-thesis-spark/documents",
        "SUP_SINGLE_INSTANCE": "",
        "SUP_PIPELINE_ID": str(pipeline)
    })
    command = [
        "docker-compose",
        "up",
        "--scale", "service=" + str(machines),
        "--exit-code-from", "submitter"
        # "-d"
    ]

    try:
        result = subprocess.check_output(" ".join(command), shell=True, env=current_environment)
        result = result.decode('unicode-escape')
        f = open('/home/simon.gehring/master-logs/submitter_uima_' + str(uima_count) + '.log', 'w')
        f.write(result)
        f.close()
    except subprocess.CalledProcessError as e:
        print(e.output.decode('unicode-escape'))


def get_date(time):
    return time.strftime('%H:%M:%S %d.%m.%Y')


def get_time_delta(time):
    return str(datetime.datetime.fromtimestamp(time))


def print_time(before=None):
    # Get seconds
    now = datetime.datetime.now()  # str().split('.')[0]
    date = get_date(now)
    if before is None:
        print(date)
    else:
        print(date + " (Needed " + str(now - before) + " sec)")
    return now


single_count = 0


def single(memory, cores, size, machines, pipeline, ratio=1.0):
    print("Single threaded with " + str(machines) + " x " + str(cores) + " = " + str(machines * cores) + " cores and " +
          str(machines) + " x " + str(memory) + " = " + str(memory * machines) + "GB memory.")
    global single_count
    single_count += 1
    log_directory = "/home/simon.gehring/master-logs/single_" + str(single_count) + "/"
    os.chdir("/home/simon.gehring/git/master-thesis-spark")
    current_environment = os.environ.copy()
    current_environment.update({
        "SUP_GUTENBERG_RATIO": str(ratio),
        "FILESIZE_MIN": str(size[0] * 1024),
        "FILESIZE_MAX": str(size[1] * 1024),
        "SUP_LOG_FILES": log_directory,
        "MEMORY_OF_SLAVE_MASTER": str(machines * memory),
        "CORES_OF_SLAVE_MASTER": str(machines * cores),
        "DOCUMENT_DIR": "/home/simon.gehring/git/master-thesis-spark/documents",
        "SUP_SINGLE_INSTANCE": "--single",
        "SUP_PIPELINE_ID": str(pipeline)
    })
    command = [
        "docker-compose",
        "-f", "compose-1m2s.yaml",
        "up",
        "--scale", "slave-two=0",
        "--exit-code-from", "submitter"
        # "-d"
    ]

    try:
        result = subprocess.check_output(" ".join(command), shell=True, env=current_environment)
        result = result.decode('unicode-escape')
        f = open('/home/simon.gehring/master-logs/submitter_single_'+str(single_count)+'.log', 'w')
        f.write(result)
        f.close()
    except subprocess.CalledProcessError as e:
        print(e.output.decode('unicode-escape'))


num_options = 1
for key in options_for_all:
    num_options *= len(options_for_all[key])

num_spark_opts = 1
for key in spark_options:
    num_spark_opts *= len(spark_options[key])

opt_count = 0
total_count = 0

for general_config in itertools.product(*options_for_all.values()):  # type: Tuple[Any]
    general_config = {k: val for k, val in zip(options_for_all, general_config)}
    opt_count += 1
    spark_opt_count = 0
    print("General config " + str(opt_count) + "/" + str(num_options))
    for spark_config in itertools.product(*spark_options.values()):  # type: Tuple[Any]
        spark_opt_count += 1
        print("Spark config " + str(spark_opt_count) + "/" + str(num_spark_opts))
        total_count += 1
        print(
            "Total " + str(total_count) + "/" + str(num_spark_opts * num_options) + " (+" + str(num_options + 1) + ")")
        spark_config = {k: val for k, val in zip(spark_options, spark_config)}
        before = print_time()
        sparkle(
            machines=general_config["Machines"],
            memory=general_config["MemoryInGB"],
            cores=general_config["Cores"],
            size=general_config["SizeInKB"],
            serializer=spark_config["Serialization"],
            compression=spark_config["Compression"],
            pipeline=general_config["PipelineId"],
            ratio=global_ratio
        )
        print_time(before)
    print("Finished Spark for current general option.")
    before=print_time()
    single(memory=general_config["MemoryInGB"], cores=general_config["Cores"], machines=general_config["Machines"],
           size=general_config["SizeInKB"],pipeline=general_config["PipelineId"], ratio=global_ratio)
    before = print_time(before)
    uimale(machines=general_config["Machines"], memory=general_config["MemoryInGB"], cores=general_config["Cores"],
           size=general_config["SizeInKB"],pipeline=general_config["PipelineId"], ratio=global_ratio)
    print_time(before)
"""

#!/bin/bash



set -e

if [ -z "$TIMEOUT_HOURS" ]; then
  echo "No timeout specified. Defaulting to 6 hours."
  TIMEOUT_HOURS=6
else
  echo "Understood minimum filesize of $FILESIZE_MIN Bytes."
fi


if [ -z "$FILESIZE_MIN" ]; then
  echo "No minimum filesize specified."
else
  echo "Understood minimum filesize of $FILESIZE_MIN Bytes."
fi
if [ -z "$FILESIZE_MAX" ]; then
  echo "No maximum filesize specified."
else
  echo "Understood maximum filesize of $FILESIZE_MAX Bytes."
fi


TIMEOUT=$((60*60*$TIMEOUT_HOURS))


# Number of workers (+1, the master slave)
export NUMBER_OF_MACHINES=11
export DOCUMENT_DIR=/home/simon.gehring/git/master-thesis-spark/documents
export MEMORY_PER_MACHINE_IN_GB=10

export CORES_PER_MACHINE=1

COMPRESSION_LOGS=with-compression/
NO_COMPRESSION_LOGS=without-compression/
SINGLE_LOGS=single-instance/
SUP_SINGLE_INSTANCE=

# Code for a console countdown timer (from https://superuser.com/questions/611538/is-there-a-way-to-display-a-countdown-or-stopwatch-timer-in-a-terminal/611582)
function countdown(){
   date1=$((`date +%s` + $1));
   while [ "$date1" -ge `date +%s` ]; do
     echo -ne "$(date -u --date @$(($date1 - `date +%s`)) +%H:%M:%S)\r";
     sleep 0.1
   done
}

echo "Starting script. It is now $(date)."


echo "Deleting old logfiles..."
sudo rm -rf logs
mkdir logs
mkdir logs/build

echo "Building shared-uima-processor..."
cd ../master-thesis-program
mvn clean install 1> ../master-thesis-spark/logs/build/shared-uima-processor.stdout.log \
2> ../master-thesis-spark/logs/build/shared-uima-processor.stderr.log

echo "Building shared-uima-benchmark..."
cd ../master-thesis-benchmark
mvn clean install 1> ../master-thesis-spark/logs/build/shared-uima-benchmark.stdout.log \
2> ../master-thesis-spark/logs/build/shared-uima-benchmark.stderr.log

echo "Moving JAR file..."
cp target/shared-uima-benchmark-0.0.1-SNAPSHOT.jar ../master-thesis-spark/jars
cd ../master-thesis-spark

echo "Removing old containers (if applicable)."
docker-compose -f compose-1m2s.yaml down --remove-orphans 1>logs/init-docker-compose-down.stdout.log \
2>logs/init-docker-compose-down.stderr.log


echo "Starting benchmark with compression (logs at $COMPRESSION_LOGS, ${MEMORY_OF_SLAVE_MASTER:-12G} + ${NUMBER_OF_MACHINES}*${MEMORY_PER_MACHINE_IN_GB}G RAM and 1 + ${NUMBER_OF_MACHINES}*${CORES_PER_MACHINE} cores)"
export SUP_COMPRESSION_ALGORITHM=gehring.uima.distributed.compression.ZLib
export SUP_LOG_FILES=$COMPRESSION_LOGS
docker-compose -f compose-1m2s.yaml up --scale slave-two=$NUMBER_OF_MACHINES -d 1>logs/compression-docker-compose-up.stdout.log \
2>logs/compression-docker-compose-up.stderr.log

echo "Successfully started benchmark. Wait for $TIMEOUT_HOURS hours..."
countdown $TIMEOUT

echo "Successfully waited (yay). Removing (hopefully) idling containers..."
docker-compose -f compose-1m2s.yaml down 1>logs/compression-docker-compose-down.stdout.log \
2>logs/compression-docker-compose-down.stderr.log

echo "Removing useless JAR files..."
sudo find logs -name \*.jar -delete

echo "Starting benchmark without compression (logs at $NO_COMPRESSION_LOGS, ${MEMORY_OF_SLAVE_MASTER:-12G} + ${NUMBER_OF_MACHINES}*${MEMORY_PER_MACHINE_IN_GB}G RAM and 1 + ${NUMBER_OF_MACHINES}*${CORES_PER_MACHINE} cores)"
export SUP_COMPRESSION_ALGORITHM=gehring.uima.distributed.compression.NoCompression
export SUP_LOG_FILES=$NO_COMPRESSION_LOGS
docker-compose -f compose-1m2s.yaml up --scale slave-two=$NUMBER_OF_MACHINES -d 1>logs/no-compression-docker-compose-up.stdout.log \
2>logs/no-compression-docker-compose-up.stderr.log

echo "Successfully started benchmark. Wait for $TIMEOUT_HOURS hours..."
countdown $TIMEOUT

echo "Successfully waited (yay). Removing (hopefully) idling containers..."
docker-compose -f compose-1m2s.yaml down -v 1>logs/no-compression-docker-compose-down.stdout.log \
2>logs/no-compression-docker-compose-down.stderr.log

echo "Removing useless JAR files..."
sudo find logs -name \*.jar -delete


export SUP_LOG_FILES=$SINGLE_LOGS
export SUP_SINGLE_INSTANCE=--single
export MEMORY_OF_SLAVE_MASTER=$(($MEMORY_PER_MACHINE_IN_GB*$(($NUMBER_OF_MACHINES+1))))G
export CORES_OF_SLAVE_MASTER=$(($CORES_PER_MACHINE*($NUMBER_OF_MACHINES+1)))
echo "Starting benchmark single instance (Log files at $SINGLE_LOGS, $MEMORY_OF_SLAVE_MASTER memory, $CORES_OF_SLAVE_MASTER cores)"
docker-compose -f compose-1m2s.yaml up --scale slave-two=0 -d 1>logs/single-docker-compose-up.stdout.log \
2>logs/single-docker-compose-up.stderr.log

echo "Successfully started benchmark. Wait for $TIMEOUT_HOURS hours..."
countdown $TIMEOUT

echo "Successfully waited (yay). Removing (hopefully) idling containers..."
docker-compose -f compose-1m2s.yaml down 1>logs/single-docker-compose-down.stdout.log \
2>logs/single-docker-compose-down.stderr.log


echo "Retrieving log files..."
export OLD_WD=$(pwd)
cd logs/${COMPRESSION_LOGS}slave-one/workspace/driver*
cp stdout $OLD_WD/logs/stdout-with-compression.log
cp stderr $OLD_WD/logs/stderr-with-compression.log
cd $OLD_WD
cd logs/${NO_COMPRESSION_LOGS}slave-one/workspace/driver*
cp stdout $OLD_WD/logs/stdout-without-compression.log
cp stderr $OLD_WD/logs/stderr-without-compression.log
cd $OLD_WD
cd logs/${SINGLE_LOGS}slave-one/workspace/driver*
cp stdout $OLD_WD/logs/stdout-single-instance.log
cp stderr $OLD_WD/logs/stderr-single-instance.log
cd $OLD_WD



echo "Formatting files..."
egrep -v "INFO|WARN" logs/stdout-with-compression.log > logs/stdout-with-compression.min.log
egrep -v "INFO|WARN" logs/stderr-with-compression.log > logs/stderr-with-compression.min.log
egrep -v "INFO|WARN" logs/stdout-without-compression.log > logs/stdout-without-compression.min.log
egrep -v "INFO|WARN" logs/stderr-without-compression.log > logs/stderr-without-compression.min.log
egrep -v "INFO|WARN" logs/stdout-single-instance.log > logs/stdout-single-instance.min.log
egrep -v "INFO|WARN" logs/stderr-single-instance.log > logs/stderr-single-instance.min.log


echo "Done with the script. It is now $(date)."
"""
