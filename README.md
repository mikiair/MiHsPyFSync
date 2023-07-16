# MiHsPyFSync
Python based comparison of two Sqlite databases containing file information (created by [MiHsPyFList](https://github.com/mikiair/MiHsPyFList) tools).

## Usage
```pfs [-h] [-c] [-o | -a] [-n | -d DOTS] source target [outfile]```

### Positional arguments
  * source - database file on source
  * target - database file on target
  * outfile - CSV or database file to write results to (default=stdout)

### Optional arguments
  * -h, --help - show help message and exit
  * -c, --ctime - consider file creation time for comparison (if present in data), default ignored

### File options
  optional arguments apply when writing to CSV or database file (ignored otherwise)

  * -o, --overwrite - overwrite the outfile if existent
  * -u, --update - update SQLite database or append to the CSV outfile if existent
  * -n, --nodots - do not display dots for matches
  * -d DOTS, --dots DOTS - logarithmic number of matching files to display one dot for (i.e. 0=every file, 1=each 10 files, 2=each 100 files...)
                        
### Requirements
Download MiHsPyFList from the above link.