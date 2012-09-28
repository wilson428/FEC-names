import json, sqlite3


gender = json.load(open("gender.json", "r"))
  
def input_genders(conn, c, mn=10):
    not_founds = c.execute("SELECT * FROM stats where gender = 'Not found' and (obama >= %i or romney >= %i) order by name" % (mn, mn)).fetchall()
    for not_found in not_founds:
        g = raw_input("guess gender for %s: " % not_found['name']).lower()
        if g == 'm' or g == 'male':
            c.execute("update stats set gender = \"male\" where name = \"%s\"" % not_found['name'])
        elif g == 'f' or g == 'female':
            c.execute("update stats set gender = \"female\" where name = \"%s\"" % not_found['name'])
        elif g == 'b':
            c.execute("update stats set gender = \"both\" where name = \"%s\"" % not_found['name'])
        elif g == 'u':
            c.execute("update stats set gender = \"unknown\" where name = \"%s\"" % not_found['name'])
        elif g == 'x':
            c.execute("update stats set gender = \"\" where name = \"%s\"" % not_found['name'])
        elif g == "exit":
            return
        conn.commit()

def get_gender(name):
    try:
        g = gender[name.title()]
    except KeyError as e:
        g = "Not found"
    return g

def save_genders(c):
    all_names = {}
    names = json.load(open("gender.json", "r"))
    for name in names:
        all_names[name.title()] = names[name]

    founds = c.execute("SELECT * FROM stats where gender = 'male' or gender = 'female' or gender = 'both'").fetchall()
    for found in founds:
        all_names[found['name'].title()] = found['gender']

    f = open("gender.json", "w")
    f.write(json.dumps(all_names, indent=3, sort_keys = True))
    f.close()
    
#save_genders()
