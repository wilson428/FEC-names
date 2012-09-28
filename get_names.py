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
conn = sqlite3.connect(path + 'all_names.sqlite')
conn.row_factory = dict_factory
c = conn.cursor()

#read the (large) csv files from the FEC into SQLite databases
#http://fec.gov/disclosurep/PDownload.do
def load_data(candidate="", filename=""):
    #make table once
    c.execute('CREATE TABLE IF NOT EXISTS donations \
                ("id" INTEGER PRIMARY KEY AUTOINCREMENT, \
                "candidate" VARCHAR(50), \
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
                "election_type" VARCHAR(10))')

    c.execute('DELETE FROM donations')
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
                    c.execute('INSERT INTO donations ("candidate", "name", "last", "first", "employer", "occupation", "city", "state", "zip", "date", "amount", "desc", "election_type") \
                            VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", "%s", %.2f, "%s", "%s" )' %
                          (line[2], line[3], last, first, line[7], line[8], line[4], line[5], line[6][0:5], dt, float(line[9]), line[11], line[17]))
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
party = {
    "Bachmann, Michele" : "Rep",
    "Cain, Herman" : "Rep",
    "Gingrich, Newt" : "Rep",
    "Huntsman, Jon" : "Rep",
    "Johnson, Gary Earl" : "Rep",
    "McCotter, Thaddeus G" : "Rep",
    "Obama, Barack" : "Dem",
    "Paul, Ron" : "Rep",
    "Pawlenty, Timothy" : "Rep",
    "Perry, Rick" : "Rep",
    "Roemer, Charles E. 'Buddy' III" : "Rep",
    "Romney, Mitt" : "Rep",
    "Santorum, Rick" : "Rep"
}
    


def get_date(dt):
    dt = dt.split("-")
    dt = "20" + dt[2] + "-" + months[dt[1]] + "-" + dt[0]
    return dt

#after raw data is loaded, make a database with every unique person
def get_names():
    c.execute('''CREATE TABLE IF NOT EXISTS "names"
               ("id" INTEGER PRIMARY KEY AUTOINCREMENT,
                "candidate" VARCHAR(50),
                "party" VARCHAR(10),
                "first" VARCHAR(25),
                "last" VARCHAR(25),
                'count' INTEGER,
                'amount' FLOAT,
                "zip" VARCHAR(10), CONSTRAINT unq UNIQUE (candidate, first, last, zip))''')

    #the FEC database lists every contribution, so many people are listed multiple times
    #there is no failsafe way to identify unique individuals, given noise in the data, but we can get close by looking at unqiue names per zipcode
    #this will undercount two people with the same name in the same zip code, but the collision space appears very low
    #this will ocercount one person who reports two different zipcodes. This also appears to occur with tolerable infrequency

    names = c.execute("SELECT candidate, first, last, zip, sum(amount), count(*) from donations group by candidate, first, last, zip order by first").fetchall()

    for name in names:            
        candidate = name['candidate']
        try:
            c.execute('''INSERT OR IGNORE INTO "names" ("candidate", "party", "first", "last", "count", "amount", "zip") VALUES (?, ?, ?, ?, ?, ?, ?)''', (candidate, party[candidate], name['first'], name['last'], name['count(*)'], name['sum(amount)'], name['zip']))
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
            
#reduces to groups of unique first names
def compile_names():
    c.execute('''CREATE TABLE IF NOT EXISTS "stats"
               ("id" INTEGER PRIMARY KEY AUTOINCREMENT,
                "name" VARCHAR(25),
                "gender" VARCHAR(15),
                "party" VARCHAR(5),
                "count" INTEGER,
                "amount" FLOAT, CONSTRAINT unq UNIQUE (name, party))''')

    c.execute('DELETE FROM stats')
    conn.commit()

    #select every unique first name
    names = c.execute("SELECT first, party, count(*) as count, sum(amount) as amount FROM names group by first, party order by first").fetchall()

    first_letter = ''
    for name in names:
        if len(name['first']) > 1:
            #track progreess
            if name['first'][0] != first_letter:
                first_letter = name['first'][0]
                print "Searching names beginning with %s..." % first_letter
                
            c.execute('INSERT INTO "stats" ("name", "gender", "party", "count", "amount") VALUES ("%s", "%s", "%s", %i, %.2f)' %
                      (name['first'], get_gender(name['first']), name['party'], name['count'], name['amount']))
        
    conn.commit()


def write_stats(threshold):
    f = open("data/stats_%i.csv" % threshold, "w")
    f.write("name,gender,total,d_count,r_count,drate,rrate,r_amount,d_amount,advantage,tilt,letter\r")

    totals = {}
    for party in c.execute("SELECT party, count(*) FROM names group by party").fetchall():
        totals[party["party"]] = party["count(*)"]

    #this joins records for the same name in different parties. It is not elegant
    #we miss names here that show up on one list but not another. Bad conceptually, OK for our purposes since any name of appreciable frequency shows up on both lists
    #surely a better way here, but I'm a JOIN novice
    names = c.execute('SELECT d.party, d.name as "name", d.gender as gender, d.count as "d_count", d.amount as "d_amount", \
                        r.party, r.name as "r_name", r.count as "r_count", r.amount as "r_amount" \
                        from stats as d LEFT OUTER JOIN stats as r ON d.name = r.name \
                        WHERE d.party = "Dem" AND r.party = "Rep" AND (d.count >= %i OR r.count >= %i) order by d.count desc' % (threshold, threshold)).fetchall()
    
    for name in names:
        l = 26 - ord(name['name'][0]) + 65        
        dc = name['d_count']
        rc = name['r_count']
        d_amount = float(name['d_amount'])
        r_amount = float(name['r_amount'])
        drate = round(1000 * float(dc) / totals["Dem"], 3)
        rrate = round(1000 * float(rc) / totals["Rep"], 3)

        if len(name['name']) > 1:
            tilt = 100 * float(rc) / (dc + rc)
            
            advantage = 100 * float(r_amount) / (d_amount + r_amount)

            if len(name['name']) > 1:
                f.write("%s,%s,%i,%i,%i,%.3f,%.3f,%.2f,%.2f,%.2f,%.1f,%i\r" % (name['name'], name['gender'], (dc + rc), dc, rc, drate, rrate, d_amount, r_amount, advantage, tilt, l))

    f.close()


#load_data("", "P00000001-ALL.csv")
#load_data("Obama", "P80003338-ALL.csv")
#load_data("Romney", "P80003353-ALL.csv")
#get_names()
#get_names("Romney")
#compile_names()
#get_genders()
write_stats(10)

conn.close()
