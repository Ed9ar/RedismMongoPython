
from pymongo import MongoClient

# pprint library is used to make the output look more pretty
from pprint import pprint
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
myclient = MongoClient("mongodb://localhost:27017/")
import re
import redis

session_username = ""

#mydb = myclient["mydatabase"]
mydb = myclient["netflix"]
#print(myclient.list_database_names())


def movieInfoRedis(conn, title):
    if not conn.sismember("movieList", title):
        #1.Si no esta llamar a Mongo
        print()
        print("NO ESTA EN REDIS VOY A MONGO ")
        print()
        pelis = movieInfoMongo(title)
        #print(pelis)
        for i in range(len(pelis)):
            #print(pelis[i]["description"])
            conn.hmset("movie:{}".format(title), {"description": str(pelis[i]["description"]), "duration": str(pelis[i]["duration"]), "type": str(pelis[i]["type"]), "rating": str(pelis[i]["rating"]), "listed_in": str(pelis[i]["listed_in"])})
            conn.expire("movie:{}".format(title), 300)
            conn.sadd("movieList", title)
            conn.expire("movieList", 300)
    print()
    print("INFO EN CACHE, MOSTRAR")
    print()
    print(title)
    print("Description :" + conn.hget("movie:{}".format(title), "description").decode("UTF-8"))
    print("Duration :" + conn.hget("movie:{}".format(title), "duration").decode("UTF-8"))
    print("Type :" + conn.hget("movie:{}".format(title), "type").decode("UTF-8")) 
    print("Rating :" + conn.hget("movie:{}".format(title), "rating").decode("UTF-8"))
    print("Listed In :" + conn.hget("movie:{}".format(title), "listed_in").decode("UTF-8"))

    #si no esta llamas movieInfoMongo
    


#PARAM QUE QUIERES SABER
def movieInfoMongo(title):
    mycol = mydb["titles"]
    pelis = []
    print("OBTENGO INFO DE MONGO ")
    for x in mycol.find({"title": str(title)},{  "_id": 0, "title": 1,"description":1, "duration": 1, "type":1 , "rating": 1, "listed_in":1 }):
        pelis.append(x)
        #pprint(x)
    print("")
    #escribir los resultados a redis
    return pelis

def actorInfoRedis(conn, actor):
    if not conn.sismember("actorList", actor):
        conn.hmset("actor:{}".format(actor), {"name": actor})
        conn.expire("actor:{}".format(actor), 300)
        conn.sadd("actorList", actor)
        conn.expire("actorList", 300)
        conn.sadd("moviesActor:{}".format(actor), actor)
        conn.expire("moviesActor:{}".format(actor), 300)
        #1.Si no esta llamar a Mongo
        print()
        print("NO ESTA EN REDIS VOY A MONGO ")
        print()
        actorInfo = actorInfoMongo(actor)
        #print(actor)
        for i in range(len(actorInfo)):
            title = str(actorInfo[i]["title"])
            ntitle = ""
            for y in range(len(title)-1):
                if(title[y] != "'" and title[y] != "[" and title[y] != "]"):
                    ntitle += title[y]
            print(ntitle)
            pelis = movieInfoMongo(ntitle)
            for i in range(len(pelis)):
                #print(pelis[i]["description"])
                conn.hmset("movie:{}".format(ntitle), {"description": str(pelis[i]["description"]), "duration": str(pelis[i]["duration"]), "type": str(pelis[i]["type"]), "rating": str(pelis[i]["rating"]), "listed_in": str(pelis[i]["listed_in"])})
                conn.expire("movie:{}".format(ntitle), 300)
                conn.sadd("movieList", ntitle)
                conn.expire("movieList", 300)
            movieActor_list = "moviesActor:{}".format(ntitle)
            conn.sadd(movieActor_list, actor)
            #conn.sadd("moviesActor:{}".format(actor), title)
            #conn.sadd("moviesActor:{}".format(actor), title)
            #moviesActor_list = "moviesActor:{}".format(title)
        #conn.sadd(moviesActor_list, actor)
      
    print()
    print("INFO EN CACHE, MOSTRAR")
    print()
    print(actor)
    print("TITLES")
    print()

    titles = conn.smembers("movieList") 
    for title in titles:
        ntitle = ""
        title = str(title)
        for x in range(1,len(title)-1):
            if(title[x] != "'" and title[x] != "[" and title[x] != "]"):
                ntitle += str(title[x])
                conn.expire("moviesActor:{}".format(ntitle), 300)
        actor1 = str(conn.smembers("moviesActor:{}".format(ntitle)))
        nactor = ""
        for x in range(2,len(actor1)-1):
            if(actor1[x] != "'" and actor1[x] != "[" and actor1[x] != "}"):
                nactor += str(actor1[x])
                conn.expire("moviesActor:{}".format(ntitle), 300)

        if nactor == actor:
            print(ntitle)
    
    #conn.expire("moviesActor:{}".format(ntitle), 20)

    return


def actorInfoMongo(nombreActor):
    mycol = mydb["filming"]
    
    pipeline = [{'$lookup': 
                {'from' : 'title',
                 'localField' : 'show_id',
                 'foreignField' : 'show_id',
                 'as' : 'year_info'}},{
                '$match':{
                        'cast':{'$regex':str(nombreActor)}
                }
                },{
                '$project':{
                    '_id':0,
                    'title':"$year_info.title"
                }
            }
    ]
    cursor = mycol.aggregate(pipeline)
    resultado = list(cursor)

    return resultado
    #escribir los resultados a redis

def directorInfoRedis(conn, director):
    if not conn.sismember("directorList", director):
        conn.hmset("director:{}".format(director), {"name": director})
        conn.expire("director:{}".format(director), 300)
        conn.sadd("directorList", director)
        conn.expire("directorList", 300)
        conn.sadd("moviesDirector:{}".format(director), director)
        conn.expire("moviesDirector:{}".format(director), 300)
        #1.Si no esta llamar a Mongo
        print()
        print("NO ESTA EN REDIS VOY A MONGO ")
        print()
        directorInfo = directorInfoMongo(director)
        #print(actor)
        for i in range(len(directorInfo)):
            title = str(directorInfo[i]["title"])
            ntitle = ""
            for y in range(len(title)-1):
                if(title[y] != "'" and title[y] != "[" and title[y] != "]"):
                    ntitle += title[y]
            print(ntitle)
            pelis = movieInfoMongo(ntitle)
            for i in range(len(pelis)):
                #print(pelis[i]["description"])
                conn.hmset("movie:{}".format(ntitle), {"description": str(pelis[i]["description"]), "duration": str(pelis[i]["duration"]), "type": str(pelis[i]["type"]), "rating": str(pelis[i]["rating"]), "listed_in": str(pelis[i]["listed_in"])})
                conn.expire("movie:{}".format(ntitle), 300)
                conn.sadd("movieList", ntitle)
                conn.expire("movieList", 300)
            movieDirector_list = "moviesDirector:{}".format(ntitle)
            conn.expire("moviesDirector:{}".format(ntitle), 300)
            conn.sadd(movieDirector_list, director)
            #conn.sadd("moviesActor:{}".format(actor), title)
            #conn.sadd("moviesActor:{}".format(actor), title)
            #moviesActor_list = "moviesActor:{}".format(title)
        #conn.sadd(moviesActor_list, actor)
      
    print()
    print("INFO EN CACHE, MOSTRAR")
    print()
    print(director)
    print("TITLES")
    print()

    titles = conn.smembers("movieList") 
    for title in titles:
        ntitle = ""
        title = str(title)
        for x in range(1,len(title)-1):
            if(title[x] != "'" and title[x] != "[" and title[x] != "]"):
                ntitle += str(title[x])
        actor1 = str(conn.smembers("moviesDirector:{}".format(ntitle)))
        nactor = ""
        for x in range(2,len(actor1)-1):
            if(actor1[x] != "'" and actor1[x] != "[" and actor1[x] != "}"):
                nactor += str(actor1[x])
            conn.expire("moviesDirector:{}".format(ntitle), 300)

        if nactor == director:
            print(ntitle)
    return
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''
    #si no esta llamas directorInfoMongo


def directorInfoMongo(nombreDirector):
    mycol = mydb["filming"]
    
    pipeline = [{'$lookup': 
                {'from' : 'title',
                 'localField' : 'show_id',
                 'foreignField' : 'show_id',
                 'as' : 'director_info'}},{
                '$match':{
                        'director':{'$regex':str(nombreDirector)}
                }
                },{
                '$project':{
                    '_id':0,
                    'title':"$director_info.title"
                }
            }
    ]
    cursor = mycol.aggregate(pipeline)
    resultado = list(cursor)

    return resultado

def anioInfoRedis(conn,anio):
    if not conn.sismember("anioList", anio):
        conn.hmset("anio:{}".format(anio), {"anio": anio})
        conn.expire("anio:{}".format(anio), 300)
        conn.sadd("anioList", anio)
        conn.expire("anioList", 300)
        conn.sadd("moviesAnio:{}".format(anio), anio)
        conn.expire("moviesAnio:{}".format(anio), 300)
        #1.Si no esta llamar a Mongo
        print()
        print("NO ESTA EN REDIS VOY A MONGO ")
        print()
        directorInfo = anioInfoMongo(anio)
        #print(actor)
        for i in range(len(directorInfo)):
            title = str(directorInfo[i]["title"])
            ntitle = ""
            for y in range(len(title)-1):
                if(title[y] != "'" and title[y] != "[" and title[y] != "]"):
                    ntitle += title[y]
            print(ntitle)
            pelis = movieInfoMongo(ntitle)
            for i in range(len(pelis)):
                #print(pelis[i]["description"])
                conn.hmset("movie:{}".format(ntitle), {"description": str(pelis[i]["description"]), "duration": str(pelis[i]["duration"]), "type": str(pelis[i]["type"]), "rating": str(pelis[i]["rating"]), "listed_in": str(pelis[i]["listed_in"])})
                conn.expire("movie:{}".format(ntitle), 300)
                conn.sadd("movieList", ntitle)
                conn.expire("movieList", 300)
            movieAnio_list = "moviesAnio:{}".format(ntitle)
            conn.expire("moviesAnio:{}".format(ntitle), 300)
            conn.sadd(movieAnio_list, anio)
            #conn.sadd("moviesActor:{}".format(actor), title)
            #conn.sadd("moviesActor:{}".format(actor), title)
            #moviesActor_list = "moviesActor:{}".format(title)
        #conn.sadd(moviesActor_list, actor)
      
    print()
    print("INFO EN CACHE, MOSTRAR")
    print()
    print(anio)
    print("TITLES")
    print()

    titles = conn.smembers("movieList") 
    for title in titles:
        ntitle = ""
        title = str(title)
        for x in range(1,len(title)-1):
            if(title[x] != "'" and title[x] != "[" and title[x] != "]"):
                ntitle += str(title[x])
        actor1 = str(conn.smembers("moviesAnio:{}".format(ntitle)))
        nactor = ""
        for x in range(2,len(actor1)-1):
            if(actor1[x] != "'" and actor1[x] != "[" and actor1[x] != "}"):
                nactor += str(actor1[x])
            conn.expire("moviesAnio:{}".format(ntitle), 300)

        if nactor == anio:
            print(ntitle)
    return
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''


def anioInfoMongo(anio):

    mycol = mydb["filming"]
    
    pipeline = [{'$lookup': 
                {'from' : 'title',
                 'localField' : 'show_id',
                 'foreignField' : 'show_id',
                 'as' : 'year_info'}},{
                '$match':{
                        'release_year':{'$regex':str(anio)}
                }
                },{
                '$project':{
                    '_id':0,
                    'title':"$year_info.title"
                }
            }
    ]
    cursor = mycol.aggregate(pipeline)
    resultado = list(cursor)

    return resultado


def paisInfoRedis(conn,pais):
    if not conn.sismember("paisList", pais):
        conn.hmset("pais:{}".format(pais), {"country": pais})
        conn.expire("pais:{}".format(pais), 300)
        conn.sadd("paisList", pais)
        conn.expire("paisList", 300)
        conn.sadd("moviesPais:{}".format(pais), pais)
        conn.expire("moviesPais:{}".format(pais), 300)
        #1.Si no esta llamar a Mongo
        print()
        print("NO ESTA EN REDIS VOY A MONGO ")
        print()
        directorInfo = paisInfoMongo(pais)
        #print(actor)
        for i in range(len(directorInfo)):
            title = str(directorInfo[i]["title"])
            ntitle = ""
            for y in range(len(title)-1):
                if(title[y] != "'" and title[y] != "[" and title[y] != "]"):
                    ntitle += title[y]
            print(ntitle)
            pelis = movieInfoMongo(ntitle)
            for i in range(len(pelis)):
                #print(pelis[i]["description"])
                conn.hmset("movie:{}".format(ntitle), {"description": str(pelis[i]["description"]), "duration": str(pelis[i]["duration"]), "type": str(pelis[i]["type"]), "rating": str(pelis[i]["rating"]), "listed_in": str(pelis[i]["listed_in"])})
                conn.expire("movie:{}".format(ntitle), 300)
                conn.sadd("movieList", ntitle)
                conn.expire("movieList", 300)
            moviePais_list = "moviesPais:{}".format(ntitle)
            conn.expire("moviesPais:{}".format(ntitle), 300)
            conn.sadd(moviePais_list, pais)
            #conn.sadd("moviesActor:{}".format(actor), title)
            #conn.sadd("moviesActor:{}".format(actor), title)
            #moviesActor_list = "moviesActor:{}".format(title)
        #conn.sadd(moviesActor_list, actor)
      
    print()
    print("INFO EN CACHE, MOSTRAR")
    print()
    print(pais)
    print("TITLES")
    print()

    titles = conn.smembers("movieList") 
    for title in titles:
        ntitle = ""
        title = str(title)
        for x in range(1,len(title)-1):
            if(title[x] != "'" and title[x] != "[" and title[x] != "]"):
                ntitle += str(title[x])
        actor1 = str(conn.smembers("moviesPais:{}".format(ntitle)))
        nactor = ""
        for x in range(2,len(actor1)-1):
            if(actor1[x] != "'" and actor1[x] != "[" and actor1[x] != "}"):
                nactor += str(actor1[x])
            conn.expire("moviesPais:{}".format(ntitle), 300)

        if nactor == pais:
            print(ntitle)
    return


def paisInfoMongo(pais):
    mycol = mydb["filming"]
    
    pipeline = [{'$lookup': 
                {'from' : 'title',
                 'localField' : 'show_id',
                 'foreignField' : 'show_id',
                 'as' : 'country_info'}},{
                '$match':{
                        'country':{'$regex':str(pais)}
                }
                },{
                '$project':{
                    '_id':0,
                    'title':"$country_info.title"
                }
            }
    ]
    cursor = mycol.aggregate(pipeline)
    resultado = list(cursor)

    return resultado
   

def generoInfoRedis(conn, genre):
    if not conn.sismember("genreList", genre):
        conn.hmset("genre:{}".format(genre), {"genre": genre})
        conn.expire("genre:{}".format(genre), 300)
        conn.sadd("genreList", genre)
        conn.expire("genreList", 300)
        conn.sadd("moviesGenre:{}".format(genre), genre)
        conn.expire("moviesGenre:{}".format(genre), 300)
        #1.Si no esta llamar a Mongo
        print()
        print("NO ESTA EN REDIS VOY A MONGO ")
        print()
        generoInfo = generoInfoMongo(genre)

        #print(actor)
        for i in range(len(generoInfo)):
            title = str(generoInfo[i]["title"])
            pelis = movieInfoMongo(title)
            for i in range(len(pelis)):
            #print(pelis[i]["description"])
                conn.hmset("movie:{}".format(title), {"description": str(pelis[i]["description"]), "duration": str(pelis[i]["duration"]), "type": str(pelis[i]["type"]), "rating": str(pelis[i]["rating"]), "listed_in": str(pelis[i]["listed_in"])})
                conn.expire("movie:{}".format(title), 300)
                conn.sadd("movieList", title)
                conn.expire("movieList", 300)
            movieGenre_list = "moviesGenre:{}".format(title)
            conn.expire("moviesGenre:{}".format(title), 300)
            conn.sadd(movieGenre_list, genre)
      
    print()
    print("INFO EN CACHE, MOSTRAR")
    print()
    print(genre)
    print("TITLES")
    print()

    titles = conn.smembers("movieList") 
    for title in titles:
        ntitle = ""
        title = str(title)
        for x in range(1,len(title)-1):
            if(title[x] != "'" and title[x] != "[" and title[x] != "]"):
                ntitle += str(title[x])
        actor1 = str(conn.smembers("moviesGenre:{}".format(ntitle)))
        nactor = ""
        for x in range(2,len(actor1)-1):
            if(actor1[x] != "'" and actor1[x] != "[" and actor1[x] != "}"):
                nactor += str(actor1[x])
            conn.expire("moviesGenre:{}".format(ntitle), 300)

        if nactor == genre:
            print(ntitle)
    return

def top10InfoRedis(conn, top):
    if not conn.sismember("topList", top):
        conn.hmset("top:{}".format(top), {"top": top})
        conn.expire("top:{}".format(top), 300)
        conn.sadd("topList", top)
        conn.expire("topList", 300)
        conn.sadd("moviesTop:{}".format(top), top)
        conn.expire("moviesTop:{}".format(top), 300)
        #1.Si no esta llamar a Mongo
        print()
        print("NO ESTA EN REDIS VOY A MONGO ")
        print()
        topInfo = top10InfoMongo()

        #print(topInfo)
        for i in range(len(topInfo)):
            title = str(topInfo[i]["_id"])
            
            conn.sadd("topList", title)
            conn.expire("topList", 300)
            movieTop_list = "moviesTop:{}".format(title)
            conn.expire("moviesTop:{}".format(title), 300)
            conn.sadd(movieTop_list, top)
      
    print()
    print("INFO EN CACHE, MOSTRAR")
    print()
    print("TOP 10")
    print()

    titles = conn.smembers("topList") 
    for title in titles:
        ntitle = ""
        title = str(title)
        for x in range(1,len(title)-1):
            if(title[x] != "'" and title[x] != "[" and title[x] != "]"):
                ntitle += str(title[x])
                conn.expire("moviesTop:{}".format(ntitle), 300)
        if(ntitle != "top"):
            print(ntitle)
    return


def top10InfoMongo():
    
    mycol = mydb["filming"]
    
    pipeline = [{'$group':{
                        '_id':"$country",
                        "number_of_countries":{'$sum': 1}
                    }
                },{
                    '$sort':{"number_of_countries":-1}
                },
                {
                    '$limit': 11
                }
    ]
    cursor = mycol.aggregate(pipeline)
    resultado = list(cursor)

    return resultado


#Aqui va el menu
conn = redis.Redis(host="localhost", port=6379)
#config get maxmemory
#config set maxmemory
ans = ""
while ans != "N":
    print("Welcome to Netflix titles app")
    print()
    print("What would you like to know today?")
    print("1. Movie info by title")
    print("2. Movies of actor")
    print("3. Movies by director")
    print("4. Titles by year")
    print("5. Titles by country")
    print("6. Titles by genre")
    print("7. Top 10 contries with more movies")
    print("8. Add movie")
    print("")

    option = input("Write the number of your choice: ")
    if int(option) == 1:
        movie = input("Write the title of the movie: ")
        movieInfoRedis(conn, movie)
        #movieInfoMongo(movie)
    elif int(option) == 2:
        actor = input("Write the name of the actor: ")
        actorInfoRedis(conn, actor)
    elif int(option) == 3:
        director = input("Write the name of the director: ")
        directorInfoRedis(conn, director)
    elif int(option) == 4:
        year = input("Write the year: ")
        anioInfoRedis(conn, year)
    elif int(option) == 5:
        country = input("Write the country: ")
        paisInfoRedis(conn, country)
    elif int(option) == 6:
        genre = input("Write the genre: ")
        generoInfoRedis(conn, genre)
    elif int(option) == 7:
        top10InfoRedis(conn, "top")
    elif int(option) == 8:
        title = str(input("Write the name of the movie: "))
        description = str(input("Write the description of the movie: "))
        typee = str(input("Write the type of the movie: "))
        rating = str(input("Write the rating of the movie: "))
        duration = int(input("Write the duration of the movie: "))
        listed_in = str(input("Write the listed in of the movie: "))
        director = str(input("Write the director in of the movie: "))
        cast = str(input("Write the cast in of the movie: "))
        country = str(input("Write the country in of the movie: "))
        date_added = str(input("Write the date added in of the movie: "))
        release_year = int(input("Write the release year in of the movie: "))
        show_id = int(input("Write the show id in of the movie: "))
        mycol = mydb["filming"]
        mycol2 = mydb["title"]
        emp_rec1 = {"title": title, "description": description, "type":typee, "rating": rating, "duration": duration, "listed_in":listed_in, "show_id": show_id}
        emp_rec2 = {"cast": cast, "country": country, "date_added":date_added, "release_year": release_year, "show_id": show_id,"director":director}
        rec_id1 = mycol2.insert_one(emp_rec2)
        rec_id2 = mycol.insert_one(emp_rec1)
        print(rec_id1.inserted_id)
        #addInfoRedis(conn, title, description,typee, )
    else:
        print("Please choose a valid option")
    
    print()
    print("Would you like to continue?")
    print("Yes = Y")
    print("No = N")
    print()
    ans = input("")
    


