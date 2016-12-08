#PGX

Just a simple wrapper around psql including environment based selection of
credentials, execute from file, automatic json formatting, and jq parsing.

To just enter psql environment with credentials, leave out file and sql args.
However, stdin/out are kind of buggy and I'm giving up on direct psql entrance
via this script. Instead, I've deferred it to a bash wrapper calling this with
the --getcommand flag and then executing

###USAGE
<pre>
usage: pgx.py [-h] [-f FILE] [-e [{p,s,d}]] [-u [U]] [-d [D]] [-l [H]] [-b]
              [-v VARIABLES [VARIABLES ...]] [--print-sql] [--print-command]
              [--no-jq] [--no-json]
              [sql] [jq]

positional arguments:
  sql                   sql to execute
  jq                    jq filter

optional arguments:
  -h, --help            show this help message and exit
  -f FILE, --file FILE  file to execute
  -e [{p,s,d}], --environment [{p,s,d}]
                        environment for execution
  -u [U], --user [U]    override environment based user
  -d [D], --db [D]      override environment based db
  -l [H], --location [H]
                        override environment based location (host)
  -b, --bash            used for hacky bash interop only (used by pgxscript)
  -v VARIABLES [VARIABLES ...], --variables VARIABLES [VARIABLES ...]
                        list of variables to replace $1 style
                        preparedarguments
  --print-sql           print the sql itself for debugging
  --print-command       print the command itself for debugging
  --no-jq               specifically opt out of jq parsing
  --no-json             specifically opt out of json return format
</pre>
