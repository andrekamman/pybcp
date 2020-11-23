from datetime import datetime
import argparse, logging, pyodbc, csv

parser = argparse.ArgumentParser()
parser.add_argument("sqlserver", help="Name of the SQL Server")
parser.add_argument("database", help="Name of the database")
parser.add_argument("table", help="Name of the table")
parser.add_argument("outputdir", help="Location for the outputfile")
parser.add_argument("-q", "--quiet", help="supress all logging info", action="store_true")
parser.add_argument("-p", "--progressindicatorvalue", help="Shows nr of rows exported", type=int, default=1000000)
parser.add_argument("-s", "--split", help="split file after x nr of records", type=int, default=-1)
parser.add_argument("-nolf", "--removecrlf", help="remove cr, lf and crlf values from export and replace them with '&#xA' & '&#xD", action="store_true")

args = parser.parse_args()

quiet = args.quiet

start_time = datetime.now()

def OpenFile(filename):
    f = open(outputfile, 'w', newline='', encoding="utf-8")
    w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
    return w


outputfile = "{outputdir}\\{table}.csv".format(outputdir=args.outputdir, table=args.table)

format = "%(asctime)s: %(message)s"
logging.basicConfig(format=format, level=logging.INFO, datefmt="%H:%M:%S")

if not quiet:
    logging.info("Connecting to database: %s on SQL Server: %s", args.database, args.sqlserver)

connection = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
    'Server=' + args.sqlserver + ';'
    'Database=' + args.database + ';'
    'Trusted_Connection=yes;'
)

metaquery = "select c.name as columnname from sys.tables t join sys.columns c on c.object_id = t.object_id where t.name = '{table}' order by c.column_id".format(table=args.table)
query = "select * from {table}".format(table=args.table)

if not quiet:
    logging.info("Getting column names for table: %s", args.table)

cursor = connection.cursor()
cursor.execute(metaquery)

header = [row.columnname for row in cursor]

cursor.execute(query)
rowcounter = 0
filecounter = 0

for row in cursor:
    if ((rowcounter % args.split == 0) or (rowcounter == 0)) and ((args.split > -1) or (rowcounter == 0)):
        filecounter += 1
        outputfile = "{outputdir}\\{table}_{nr}.csv".format(outputdir=args.outputdir, table=args.table, nr=filecounter)
        w = OpenFile(outputfile)

        if not quiet:
            logging.info("Starting new file: %s", outputfile)
        w.writerow(header)

    if args.removecrlf:
        newrow = [column.replace("\r\n","&#xD;&#xA;").replace("\r","&#xD;").replace("\n", "&#xA;") if type(column) == str else column for column in row]
    else:
        newrow = row             

    w.writerow(newrow)
    
    rowcounter += 1
    if (rowcounter % args.progressindicatorvalue == 0):
        if not quiet:
            logging.info("     Exported %s rows", rowcounter)


