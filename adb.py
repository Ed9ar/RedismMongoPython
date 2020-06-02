
from pymongo import MongoClient

# pprint library is used to make the output look more pretty
from pprint import pprint
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
myclient = MongoClient("mongodb://localhost:27017/")
import re
import redis

session_username = ""

#mydb = myclient["mydatabase"]
mydb = myclient["Netflix"]
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
            conn.sadd("movieList", title)
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
        conn.sadd("actorList", actor)
        conn.sadd("moviesActor:{}".format(actor), actor)
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
                conn.sadd("movieList", ntitle)
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
        actor1 = str(conn.smembers("moviesActor:{}".format(ntitle)))
        nactor = ""
        for x in range(2,len(actor1)-1):
            if(actor1[x] != "'" and actor1[x] != "[" and actor1[x] != "}"):
                nactor += str(actor1[x])

        if nactor == actor:
            print(ntitle)

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
        conn.sadd("directorList", director)
        conn.sadd("moviesDirector:{}".format(director), director)
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
                conn.sadd("movieList", ntitle)
            movieDirector_list = "moviesDirector:{}".format(ntitle)
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
        conn.sadd("anioList", anio)
        conn.sadd("moviesAnio:{}".format(anio), anio)
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
                conn.sadd("movieList", ntitle)
            movieAnio_list = "moviesAnio:{}".format(ntitle)
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
        conn.sadd("paisList", pais)
        conn.sadd("moviesPais:{}".format(pais), pais)
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
                conn.sadd("movieList", ntitle)
            moviePais_list = "moviesPais:{}".format(ntitle)
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
   

def generoInfoRedis(conn, pais):
    if not conn.sismember("genreList", pais):
        conn.hmset("genre:{}".format(pais), {"genre": pais})
        conn.sadd("genreList", pais)
        conn.sadd("moviesGenre:{}".format(pais), pais)
        #1.Si no esta llamar a Mongo
        print()
        print("NO ESTA EN REDIS VOY A MONGO ")
        print()
        directorInfo = generoInfoMongo(pais)
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
                conn.hmset("movie:{}".format(title), {"description": str(pelis[i]["description"]), "duration": str(pelis[i]["duration"]), "type": str(pelis[i]["type"]), "rating": str(pelis[i]["rating"]), "listed_in": str(pelis[i]["listed_in"])})
                conn.sadd("movieList", title)
            movieGenre_list = "moviesGenre:{}".format(ntitle)
            conn.sadd(movieGenre_list, pais)
      
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
        actor1 = str(conn.smembers("moviesGenre:{}".format(ntitle)))
        nactor = ""
        for x in range(2,len(actor1)-1):
            if(actor1[x] != "'" and actor1[x] != "[" and actor1[x] != "}"):
                nactor += str(actor1[x])

        if nactor == pais:
            print(ntitle)
    return


def generoInfoMongo(genre):
    
    mycol = mydb["title"]
    
    pipeline = [{'$match':{
                        'listed_in':str(genre)
                }
                },{
                '$project':{
                    '_id':0,
                    'title':1
                }
            }
    ]
    cursor = mycol.aggregate(pipeline)
    resultado = list(cursor)

    return resultado


#Aqui va el menu
conn = redis.Redis(host="localhost", port=6379)
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
    else:
        print("Please choose a valid option")
    
    print()
    print("Would you like to continue?")
    print("Yes = Y")
    print("No = N")
    print()
    ans = input("")
    


