import sqlite3, csv, re

path = "/Users/cewilson/Desktop/source/FEC/"
months = { "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12" }

#optional: returns sqlite queries as dictionaries instead of lists
def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

#establish database connection
conn = sqlite3.connect(path + 'names.sqlite')
conn.row_factory = dict_factory
c = conn.cursor()

def split_name(name):
    #remove suffixes and salutations that occur after surname in some records
    #this is tailored to problems I found in FEC records, not a general solution
    name = name.replace(".", "").replace("DR ","").replace(",DR", "").replace("REV ", "").replace(", PHD","").replace(",PHD", "").replace(", III","").replace(", JR","").replace(", SR", "").replace(", MD", "").title()

    parts = re.split(",+", name) #sometimes two quotes end up adjacent, but doesn't appear to denote blank field
    last = parts[0]
    if len(parts) > 1:
        first = re.split("[\s,]+", parts[1].strip())[0].replace("(", "").replace(")", "")
    else:
        first = None
    return last, first

def get_date(dt):
    dt = dt.split("-")
    dt = "20" + dt[2] + "-" + months[dt[1]] + "-" + dt[0]
    return dt

def load_data(candidate, filename):
    #make table once
    c.execute('CREATE TABLE IF NOT EXISTS %s \
                ("id" INTEGER PRIMARY KEY AUTOINCREMENT, \
                "name" VARCHAR(50), \
                "first" VARCHAR(30), \
                "last" VARCHAR(30), \
                "employer" VARCHAR(50), \
                "occupation" VARCHAR(50), \
                "city" VARCHAR(50), \
                "state" VARCHAR(50), \
                "zip" VARCHAR(10), \
                "date" DATE, \
                "amount" FLOAT, \
                "desc" VARCHAR(20), \
                "memo_code" VARCHAR(10), \
                "memo_text" VARCHAR(50), \
                "form_type" VARCHAR(10), \
                "file_num" VARCHAR(10), \
                "tran_id" VARCHAR(10), \
                "election_type" VARCHAR(10))' % candidate)

    c.execute('DELETE FROM %s' % candidate)
    conn.commit()

    #this is a large file, so we don't want to load it all into memory with .read() or .readlines()
    #format: cmte_id,cand_id,cand_nm,contbr_nm,contbr_city,contbr_st,contbr_zip,contbr_employer,contbr_occupation,contb_receipt_amt,contb_receipt_dt,receipt_desc,memo_cd,memo_text,form_tp,file_num,tran_id,election_tp

    count = 1
    print count
    if count % 100 == 0:
        conn.commit()
        return

    for line in csv.reader(open(path + filename, "r"), delimiter=",", quotechar='"'):
        if line[0] != 'cmte_id':
            last, first = split_name(line[3])
            date = get_date(line[10])

            #note: cut off zip after 5 digits and remove periods from names, to avoid double-counting due to inconsistent data entry
            try:
                c.execute('INSERT INTO %s ("name", "last", "first", "employer", "occupation", "city", "state", "zip", \
                            "date", "amount", "desc", "memo_code", "memo_text", "form_type", "file_num", "tran_id", "election_type") \
                         VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", %.2f, "%s", "%s", "%s", "%s", "%s", "%s", "%s" )' %
                          (candidate, line[3], last, first, line[7], line[8], line[4], line[5], line[6][0:5], date, float(line[9]), line[11], line[12], line[13], line[14], line[15], line[16], line[17]))
            except Exception as e:
                print line, e
            count = count + 1
    conn.commit()       

#some basic queries to compare against fec.gov maps
def check_data(candidate):
    for st in c.execute("SELECT state, sum(amount) FROM %s group by state" % candidate).fetchall():
        print st

    
def get_names(candidate):
    c.execute('''CREATE TABLE IF NOT EXISTS "names"
               ("id" INTEGER PRIMARY KEY AUTOINCREMENT,
                "candidate" VARCHAR(50),
                "first" VARCHAR(25),
                "last" VARCHAR(25),
                "zip" VARCHAR(10), CONSTRAINT unq UNIQUE (first, last, zip))''')

    #the FEC database lists every contribution, so many people are listed multiple times
    #there is no failsafe way to identify unique individuals, given noise in the data, but we can get close by looking at unqiue names per zipcode
    #this will undercount two people with the same name in the same zip code, but the collision space appears very low
    #this will ocercount one person who reports two different zipcodes. This also appears to occur with tolerable infrequency

    names = c.execute("SELECT first, last, zip, count(*) from %s group by name, zip order by name" % candidate).fetchall()
    print len(names)

    for name in names:            
        try:
            c.execute('''INSERT OR IGNORE INTO "names" ("candidate", "first", "last", "zip") VALUES (?, ?, ?, ?)''', (candidate, first, last, name['zip']))
        except Exception as e:
            print last, first, e
    conn.commit()

def compile_names():
    c.execute('''CREATE TABLE IF NOT EXISTS "stats"
               ("id" INTEGER PRIMARY KEY AUTOINCREMENT,
                "name" VARCHAR(25) UNIQUE,
                "gender" VARCHAR(15),
                "obama" INTEGER,
                "romney" INTEGER)''')

    c.execute('DELETE FROM stats')
    conn.commit()    

    names = c.execute("SELECT first, count(*) FROM names group by first order by count(*) desc").fetchall()
    for name in names:
        split = c.execute("SELECT first, candidate, count(*) FROM names WHERE first = \"%s\" group by candidate order by candidate" % name['first']).fetchall()

        oc = 0
        rc = 0
        for s in split:
            if s['candidate'] == "Obama":
                oc = s['count(*)']
            elif s['candidate'] == "Romney":
                rc = s['count(*)']

        c.execute('INSERT INTO "stats" ("name", "gender", "obama", "romney") VALUES ("%s", "%s", %i, %i)' % (name['first'], get_gender(name['first']), oc, rc))
        
    conn.commit()


def write_stats(threshhold):
    f = open("stats_%i.csv" % threshhold, "w")
    f.write("name,gender,total,obama_count,romney_count,obama_ratio,romney_ratio,split,tilt\r")

    totals = {}
    for cand in c.execute("SELECT candidate, count(*) FROM names group by candidate").fetchall():
        totals[cand["candidate"]] = cand["count(*)"]

    names = c.execute("SELECT * FROM stats").fetchall()
    
    for name in names:
        oc = name['obama']
        rc = name['romney']
        o = round(1000 * float(oc) / totals["Obama"], 3)
        r = round(1000 * float(rc) / totals["Romney"], 3)

        if len(name['name']) > 1 and (oc >= threshhold or rc >= threshhold):
            if rc == 0:
                ratio = 100
            else:
                ratio = round(o / r, 3)

            tilt = float(oc) / (oc + rc)

            if len(name['name']) > 1:
                f.write("%s,%s,%i,%i,%i,%.3f,%.3f,%.3f,%.3f\r" % (name['name'], name['gender'], (oc + rc), oc, rc, o, r, ratio, tilt))

    f.close()



load_data("Obama", "P80003338-ALL.csv")
#load_data("Romney", "P80003353-ALL.csv")
#get_names("Obama")
#get_names("Romney")
#write_stats(1000)
#compile_names()
#hyphens()   
conn.close()
