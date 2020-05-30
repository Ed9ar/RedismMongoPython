
from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint
# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
myclient = MongoClient("mongodb://localhost:27017/")

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
    #pensar parametros
    print(title)
    for x in mycol.find({"title": str(title)},{  "_id": 0, "title": 1,"description":1, "duration": 1, "type":1 , "rating": 1, "listed_in":1 }):
        pprint(x)
        print()
    #escribir los resultados a redis

def actorInfoRedis():
    return
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''
     #si no esta llamas actorInfoMongo


def actorInfoMongo(nombreActor):
    return
    #regresar pelis en que ha estado
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
    #regresar pelis que ha dirigido
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
        print(x)
    #escribir los resultados a redis

def anioInfoRedis():
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''


def anioInfoMongo(anio):
    #regresar pelis de ese anio
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
        print(x)
    #escribir los resultados a redis

def paisInfoRedis():
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''


def paisInfoMongo(pais):
    #regresar pelis de ese pais
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
        print(x)
    #escribir los resultados a redis

def ratingInfoRedis():
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''


def ratingInfoMongo(bajoOalto, cuantos):
    #dependiendo del param regresar top ratings mas bajo y mas alto limit cuantos
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
        print(x)
    #escribir los resultados a redis

def generoInfoRedis():
    '''
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
    print(x)'''


def generoInfoMongo(genero):
    #dependiendo del param regresar top ratings mas bajo y mas alto limit cuantos
    mycol = mydb["titles"]
    #pensar parametros
    for x in mycol.find({},{ "_id": 0, "title": 1, "duration": 1, "type":1 }):
        print(x)
    #escribir los resultados a redis


#Aqui va el menu
ans = ""
while ans != "N":
    print("Welcome to Netflix titles app")
    print()
    print("What would you like to know today?")
    print("1. Movie info by title")
    print("2. Actor info by name")
    print("3. Director info by name")
    print("4. Titles by year")
    print("5. Titles by country")
    print("6. Titles by rating")
    print("7. Titles by genre")
    print("")

    option = input("Write the number of your choice: ")
    if int(option) == 1:
        movie = input("Write the title of the movie: ")
        #movieInfoRedis(movie)
        movieInfoMongo(movie)
    elif int(option) == 2:
        actor = input("Write the name of the actor: ")
    elif int(option) == 3:
        director = input("Write the name of the director: ")
    elif int(option) == 4:
        year = input("Write the year: ")
    elif int(option) == 5:
        country = input("Write the country: ")
    elif int(option) == 6:
        print("Highest or Lowest ?")
        print("Highest = H")
        print("Lowest = L")
        horl = input("")
    elif int(option) == 7:
        genre = input("Write the genre: ")
    else:
        print("Please choose a valid option")
    
    print()
    print("Would you like to continue?")
    print("Yes = Y")
    print("No = N")
    print()
    ans = input("")
    


