import sqlite3, csv, re
from gender import *

path = "/Users/cewilson/Desktop/source/FEC/"

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

#read the (large) csv files from the FEC into SQLite databases
#http://fec.gov/disclosurep/PDownload.do
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
                "election_type" VARCHAR(10))' % candidate)

    c.execute('DELETE FROM %s' % candidate)
    conn.commit()

    #this is a large file, so we don't want to load it all into memory with .read() or .readlines()
    #format: cmte_id,cand_id,cand_nm,contbr_nm,contbr_city,contbr_st,contbr_zip,contbr_employer,contbr_occupation,contb_receipt_amt,contb_receipt_dt,receipt_desc,memo_cd,memo_text,form_tp,file_num,tran_id,election_tp

    count = 1
    for line in csv.reader(open(path + filename, "r"), delimiter=",", quotechar='"'):
        if count % 10000 == 0:
            print "added %i records" % count
            conn.commit()

        if line[0] != 'cmte_id':
            last, first = split_name(line[3])
            #important to exclude those without first name -- often it's a big campaign expenditure 
            if first != "":
                dt = get_date(line[10])
                #note: cut off zip after 5 digits and remove periods from names, to avoid double-counting due to inconsistent data entry
                try:
                    c.execute('INSERT INTO %s ("name", "last", "first", "employer", "occupation", "city", "state", "zip", "date", "amount", "desc", "election_type") \
                            VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", %.2f, "%s", "%s" )' %
                          (candidate, line[3], last, first, line[7], line[8], line[4], line[5], line[6][0:5], dt, float(line[9]), line[11], line[17]))
                except Exception as e:
                    print line, e
                count = count + 1
    conn.commit()       
    print "Added a total of %i records for %s" % (count, candidate)

#The names arrive as strings, so we need to intelligently split into first and last names
def split_name(name):
    #remove suffixes and salutations that occur after surname in some records
    #this is tailored to problems I found in FEC records, not a general solution
    name = name.replace(".", "").replace("DR ","").replace(",DR", "").replace("REV ", "").replace(", PHD","").replace(",PHD", "").replace(", III","").replace(", JR","").replace(", SR", "").replace(", MD", "").title()

    parts = re.split(",+", name) #sometimes two quotes end up adjacent, but doesn't appear to denote blank field
    last = parts[0]
    if len(parts) > 1:
        first = re.split("[\s,]+", parts[1].strip())[0].replace("(", "").replace(")", "")
    else:
        first = ""
    return last, first

#convert date to SQL format
#http://www.sqlite.org/lang_datefunc.html
months = { "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12" }

def get_date(dt):
    dt = dt.split("-")
    dt = "20" + dt[2] + "-" + months[dt[1]] + "-" + dt[0]
    return dt

#after raw data is loaded, make a database with every unique person
def get_names(candidate):
    c.execute('''CREATE TABLE IF NOT EXISTS "names"
               ("id" INTEGER PRIMARY KEY AUTOINCREMENT,
                "candidate" VARCHAR(50),
                "first" VARCHAR(25),
                "last" VARCHAR(25),
                'count' INTEGER,
                'amount' FLOAT,
                "zip" VARCHAR(10), CONSTRAINT unq UNIQUE (first, last, zip))''')

    #the FEC database lists every contribution, so many people are listed multiple times
    #there is no failsafe way to identify unique individuals, given noise in the data, but we can get close by looking at unqiue names per zipcode
    #this will undercount two people with the same name in the same zip code, but the collision space appears very low
    #this will ocercount one person who reports two different zipcodes. This also appears to occur with tolerable infrequency

    names = c.execute("SELECT first, last, zip, sum(amount), count(*) from %s group by first, last, zip order by first" % candidate).fetchall()

    for name in names:            
        try:
            c.execute('''INSERT OR IGNORE INTO "names" ("candidate", "first", "last", "count", "amount", "zip") VALUES (?, ?, ?, ?, ?, ?)''', (candidate, name['first'], name['last'], name['count(*)'], name['sum(amount)'], name['zip']))
        except Exception as e:
            print last, first, e
    conn.commit()

def get_genders():
    not_founds = c.execute("SELECT * FROM stats where gender = 'Not found'").fetchall()
    for not_found in not_founds:
        h = not_found['name'].split('-')
        if len(h) > 1:
            g0 = get_gender(h[0])
            g1 = get_gender(h[1])
            if g0 == g1 and (g0 == "male" or g0 == "female"):
                c.execute("update stats set gender = \"%s\" where name = \"%s\"" % (g0, not_found['name']))
            conn.commit()
        else:
            g = get_gender(h[0])
            if g == "male" or g == "female":
                c.execute("update stats set gender = \"%s\" where name = \"%s\"" % (g, not_found['name']))
            conn.commit()
            
#reduces to groups of first names
def compile_names():
    c.execute('''CREATE TABLE IF NOT EXISTS "stats"
               ("id" INTEGER PRIMARY KEY AUTOINCREMENT,
                "name" VARCHAR(25) UNIQUE,
                "gender" VARCHAR(15),
                "obama" INTEGER,
                "romney" INTEGER,
                "amt_obama" FLOAT,
                "amt_romney" FLOAT)''')

    c.execute('DELETE FROM stats')
    conn.commit()    

    names = c.execute("SELECT first, count(*) FROM names group by first order by count(*) desc").fetchall()
    for name in names:
        if len(name['first']) > 1:
            split = c.execute("SELECT first, candidate, count(*), sum(amount) FROM names WHERE first = \"%s\" group by candidate order by candidate" % name['first']).fetchall()

            oc = 0
            rc = 0
            for s in split:
                if s['candidate'] == "Obama":
                    oc = s['count(*)']
                    o_amt = s['sum(amount)']
                elif s['candidate'] == "Romney":
                    rc = s['count(*)']
                    r_amt = s['sum(amount)']

            c.execute('INSERT INTO "stats" ("name", "gender", "obama", "romney", "amt_obama", "amt_romney") VALUES ("%s", "%s", %i, %i, %.2f, %.2f)' % (name['first'], get_gender(name['first']), oc, rc, o_amt, r_amt))
        
    conn.commit()


def write_stats(threshhold):
    f = open("data/stats_%i.csv" % threshhold, "w")
    f.write("name,gender,total,obama_count,romney_count,obama_ratio,romney_ratio,obama_amt,romney_amt,advantage,split,tilt,letter\r")

    totals = {}
    for cand in c.execute("SELECT candidate, count(*) FROM names group by candidate").fetchall():
        totals[cand["candidate"]] = cand["count(*)"]

    names = c.execute("SELECT * FROM stats").fetchall()
    
    for name in names:
        l = 26 - ord(name['name'][0]) + 65        
        oc = name['obama']
        rc = name['romney']
        oamt = float(name['amt_obama'])
        ramt = float(name['amt_romney'])
        o = round(1000 * float(oc) / totals["Obama"], 3)
        r = round(1000 * float(rc) / totals["Romney"], 3)

        if len(name['name']) > 1 and (oc >= threshhold or rc >= threshhold):
            if rc == 0:
                ratio = 100
            else:
                ratio = round(o / r, 3)

            tilt = 100 * float(oc) / (oc + rc)
            advantage = 100 * float(oamt) / (oamt + ramt)

            if len(name['name']) > 1:
                f.write("%s,%s,%i,%i,%i,%.2f,%.2f,%.3f,%.3f,%.3f,%.3f,%.3f,%i\r" % (name['name'], name['gender'], (oc + rc), oc, rc, o, r, oamt, ramt, advantage, ratio, tilt, l))

    f.close()



#load_data("Obama", "P80003338-ALL.csv")
#load_data("Romney", "P80003353-ALL.csv")
#get_names("Obama")
#get_names("Romney")
#compile_names()
#get_genders()
write_stats(10)

conn.close()
