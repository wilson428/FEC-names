import sqlite3, json, re

#optional: returns sqlite queries as dictionaries instead of lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

#establish database connection
conn = sqlite3.connect('names.sqlite')
conn.row_factory = dict_factory
c = conn.cursor()

def load_data(candidate, filepath):
    #make table once
    c.execute('''CREATE TABLE IF NOT EXISTS "FEC"
               ("id" INTEGER PRIMARY KEY AUTOINCREMENT,
                "candidate" VARCHAR(50),
                "name" VARCHAR(50),
                "employer" VARCHAR(50),
                "occupation" VARCHAR(50),
                "city" VARCHAR(50),
                "state" VARCHAR(50),
                "zip" VARCHAR(10),
                "date" VARCHAR(50),
                "amount" INTEGER)''')

    #open json data file from FEC (http://www.fec.gov/finance/disclosure/candcmte_info.shtml)
    #it's also available in CSV, but I find parsing CSV to be a headache, and one missing field screws up the entire line

    #this is a large file, so we don't want to load it all into memory with .read() or .readlines()
    for line in open(filepath, "r"):
        if line.split(" ")[0] == '"transaction":':
            #make line self-contained JSON
            line = line.replace('"transaction": {', "").replace("},", "")
            #the FEC file has some annoying backslashes and tabs that trip up the JSON parser
            line = line.replace("\\", "/").replace("\t", ",")
            try:
                transaction = json.loads("{" + line + "}")
            except ValueError as ve:
                transaction = None
                print line, ve
            if transaction:
                parts = re.split("[\s,]+", transaction["Contributor Name"])

                #note: cut off zip after 5 digits and remove periods from names, to avoid double-counting due to inconsistent data entry
                c.execute('''INSERT INTO "FEC" ("candidate", "name", "employer", "occupation", "city", "state", "zip", "date", "amount")
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''', (candidate, transaction["Contributor Name"].replace(".", "").title(), transaction["Employer"], transaction["Occupation"], transaction["City"], transaction["State"], transaction["Zip"][0:5], transaction["Receipt Date"], transaction["Amount"]))        
    conn.commit()


def get_names(candidate):
    c.execute('''CREATE TABLE IF NOT EXISTS "names"
               ("id" INTEGER PRIMARY KEY AUTOINCREMENT,
                "candidate" VARCHAR(50),
                "first" VARCHAR(25),
                "last" VARCHAR(25),
                "zip" VARCHAR(10), CONSTRAINT unq UNIQUE (first, last, zip))''')

    #the FEC database lists every contribution, so many people are listed multiple times
    #there is no failsafe way to identify unique individuals, given noise in the data, but we can get close by looking at unqiue names per city
    #this will undercount two people with the same name in the same city, but the collision space appears very low
    names = c.execute("SELECT name, zip, count(*) from FEC where candidate = '%s' group by name, zip order by name" % candidate).fetchall()
    print len(names)

    for name in names:
        parts = re.split("[\s,]+", name['name'])
        if len(parts) > 1:
            last, first = parts[0], parts[1]
            c.execute('''INSERT OR IGNORE INTO "names" ("candidate", "first", "last", "zip") VALUES (?, ?, ?, ?)''', (candidate, first, last, name['zip']))
    conn.commit()
    
#load_data()
#get_names("Obama")

conn.close()
