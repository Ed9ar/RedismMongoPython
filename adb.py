
from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
myclient = MongoClient("mongodb://localhost:27017/")
import re

#mydb = myclient["mydatabase"]
mydb = myclient["Netflix"]
#print(myclient.list_database_names())


def movieInfoRedis(title):
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''
    #si no esta llamas movieInfoMongo
    movieInfoMongo(title)


#PARAM QUE QUIERES SABER
def movieInfoMongo(title):
    mycol = mydb["titles"]
    for x in mycol.find({"title": str(title)},{  "_id": 0, "title": 1,"description":1, "duration": 1, "type":1 , "rating": 1, "listed_in":1 }):
        pprint(x)
    #escribir los resultados a redis
    return

def actorInfoRedis():
    return
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''
     #si no esta llamas actorInfoMongo


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
    pprint(list(cursor))
    
    return
    #escribir los resultados a redis

def directorInfoRedis():
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
    pprint(list(cursor))
    
    #escribir los resultados a redis
    return

def anioInfoRedis():
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
    pprint(list(cursor))
    #escribir los resultados a redis
    return


def paisInfoRedis():
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''


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
    pprint(list(cursor))
    #escribir los resultados a redis
    return
   

def ratingInfoRedis():
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''


def ratingInfoMongo(title):
    mycol = mydb["title"]
    
    pipeline = [{'$match':{
                        'title':str(title)
                }
                },{
                '$project':{
                    '_id':0,
                    'rating':1
                }
            }
    ]
    cursor = mycol.aggregate(pipeline)
    pprint(list(cursor))
    #escribir los resultados a redis
    return

def generoInfoRedis():
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''


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
    pprint(list(cursor))
    #escribir los resultados a redis
    return


#Aqui va el menu
ans = ""
while ans != "N":
    print("Welcome to Netflix titles app")
    print()
    print("What would you like to know today?")
    print("1. Movie info by title")
    print("2. Movies of actor")
    print("3. Director info by name")
    print("4. Titles by year")
    print("5. Titles by country")
    print("6. Rating by title")
    print("7. Titles by genre")
    print("")

    option = input("Write the number of your choice: ")
    if int(option) == 1:
        movie = input("Write the title of the movie: ")
        #movieInfoRedis(movie)
        movieInfoMongo(movie)
    elif int(option) == 2:
        actor = input("Write the name of the actor: ")
        actorInfoMongo(actor)
    elif int(option) == 3:
        director = input("Write the name of the director: ")
        directorInfoMongo(director)
    elif int(option) == 4:
        year = input("Write the year: ")
        anioInfoMongo(year)
    elif int(option) == 5:
        country = input("Write the country: ")
        paisInfoMongo(country)
    elif int(option) == 6:
        rating = input("Write the title:")
        ratingInfoMongo(rating)
    elif int(option) == 7:
        genre = input("Write the genre: ")
        generoInfoMongo(genre)
    else:
        print("Please choose a valid option")
    
    print()
    print("Would you like to continue?")
    print("Yes = Y")
    print("No = N")
    print()
    ans = input("")
    


