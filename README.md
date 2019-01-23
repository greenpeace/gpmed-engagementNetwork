# gpmed-engagementNetwork
Script(s) to pipe third-party data into a MySQL server. 
__Note__: the used `python3` version is `3.5.2`. All mentions of `python` below refer to this version as well, but you need to adjust them according to your system (`python3` on most systems except e.g. Arch Linux or in virtual environments). 

```console
$ python en_manual.py --help
usage: en_manual.py [-h] [--start START] [--end END] [-c CONFIG] [-s] [-l LOG]

optional arguments:
  -h, --help            show this help message and exit
  --start START         Specify the starting date for data collection in
                        MMDDYYY format.
  --end END             Specify the ending date for data collection in MMDDYYY
                        format.
  -c CONFIG, --config CONFIG
                        Specify the path to configuration file, containing a
                        tokenstring and the MySQL server parameters.
  -s, --silent          No progress output to console, only critical errors
                        will be printed.
  -l LOG, --log LOG     Specify a logfile.
```

### script documentation
There is a docstring in every function, shortly summarizing it's workings. The `main` function contains a longer description of how the date range is build. 

### crontab entry for the script(s)
Open the `crontab` file with `$ crontab -e` and add:
```console
0 6 * * * "$(command -v bash)" -c 'python /var/python_files/en_manual.py \
                                          -c /var/python_files/config.json > /tmp/en_log.txt 2>&1'
```
This ensures the script is run every day at 06:00 o'clock, and the scripts output - both standard and error output (`2>&1`) - are piped into a textfile `/tmp/en_log.txt`. (The newline and `\` are not needed - they are here for readability.)

### dependency table 
Packages like `datetime`, `logging`, and `argparse` should ship in a compatible version with a python version >= `3.5.2`.

| package 		  | version   |
|:-----------------------:| ---------:|
| `jsonschema` 		  | 2.0.9     |
| `beautifulsoup4`  	  | 4.6.0     |
| `lxml` 		  | 4.3.0     |
| `requests` 		  | 2.9.1     |
| `mysql-connector` 	  | 8.0.5b1   |

Install packages on the command line via `pip` (or `pip3`, depending on your setup) with:
```console
$ pip install -r requirements.txt
```
