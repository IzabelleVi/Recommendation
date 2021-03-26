import mysql.connector
import random
import time
import sys
import collections


def connect_met_database():
    """
    In deze functie meld je je aan aan de sql database, deze functie wordt vaker in het programma opgeroepen door andere
    functies.  Je returned de connectie in de cursor om opdrachten mee uit te voeren.

    :return connectie, cursor:
    """


    #Variabele om in te loggen
    host_naam = "localhost"
    gebruikersnaam = "root"
    ww = ""
    global database_naam
    database_naam = "test"

    #connectie met de database
    connectie = mysql.connector.connect(host=host_naam,
                                        user=gebruikersnaam,
                                        password=ww,
                                        database=database_naam)
    cursor = connectie.cursor()

    return connectie, cursor

def database_sluiten(connectie, cursor):
    """
    Hier sluit je de cursor, commit je data en sluit je daarna ook de database. Ook deze functie wordt alleen door
    andere functies opgeroepen.
    :param connectie:
    :param cursor:
    """

    cursor.close()
    connectie.commit()
    connectie.close()

def maak_tabellen_aan(cursor):
    """
    Deze functie is die die de tabellen in de database aanmaakt. Hij maakt plaats voor het ID van de 4 producten per
    recommendation die je later aan de tabellen gaat toevoegen.
    :param cursor:
    """
    cursor.execute("USE " + database_naam)
    cursor.execute("CREATE TABLE main_category_rec (id VARCHAR(255) PRIMARY KEY UNIQUE, product1 VARCHAR(255) NULL, product2 VARCHAR(255) NULL, product3 VARCHAR(255) NULL, product4 VARCHAR(255) NULL)")

def verwijder_tabellen(cursor):
    """
    Deze functie verwijderd de tabellen van de recommendations die zijn gemaakt.
    :param cursor
    """
    cursor.execute("USE " + database_naam)
    cursor.execute("DROP TABLE main_category_rec")

def profielen_db(cursor):
    """
    :param cursor:
    :return:
    """
    cursor.execute(
        "SELECT DISTINCT sessions.profiles_id_key FROM orders, sessions WHERE sessions.id = orders.sessions_id_key")
    profielen = cursor.fetchall()

    return profielen


def data_ophalen_uit_database(cursor, profiel_id):
    """
    functie voor het krijgen en returnen van profiel_data
    :return profiel_data
    :param cursor, profiel_id
    """
    profiel_cat = []
    cursor.execute("SELECT products.id,products.price,products.stock,main_category.id,orders.aantal,brand.brand,gender.id,orders.sessions_id_key,doelgroep.id,sessions.profiles_id_key FROM `products`,`brand`,`gender`,`orders`,`main_category`,`sessions`,`doelgroep` WHERE products.gender_id_key = gender.id AND products.main_category_id_key = main_category.id AND products.brand_id_key = brand.id AND orders.products_id_key = products.id AND products.doelgroep_id_key = doelgroep.id AND orders.sessions_id_key = sessions.id AND profiles_id_key = '%s'" % profiel_id)
    profiel_data = cursor.fetchall()
    for item in profiel_data:
        profiel_cat.append(item[3])

    return profiel_cat

def meest_voorkomend(cursor, profiel_id):
    counter = 0
    data =data_ophalen_uit_database(cursor, profiel_id)
    num = data[0]

    for i in data:
        curr_frequency = data.count(i)
        if (curr_frequency > counter):
            counter = curr_frequency
            num = i
    return num

def tabelInformatie(tabel, cursor):
    cursor.execute("SELECT * FROM " + tabel)
    ophaal = cursor.fetchall()

    return ophaal

def main_category_rec(cursor, profiel_id):
    """
    functie voor het maken van content recommendation gebasseerd op producten die lijken op wat er laatst is gekocht
    :param cursor
    :param profielid
    :return profiel_id, random 4 recommendated products
    """
    main_category = meest_voorkomend(cursor, profiel_id)
    data = tabelInformatie('products', cursor)

    categoryKeys = []
    for item in data:
        if item[10] == main_category:
            categoryKeys.append(item[0])
    # product_ids = []
    # Ophalen data uit database

        # for p in categoryKeys:
    # cursor.execute("SELECT `id`, `main_category_id_key` FROM `products` WHERE main_category_id_key = '{0}' ".format([main_category]))
    # recommendations = cursor.fetchall()
    # print(recommendations)
    #
    # for i in recommendations:
    #     product_ids.append(i[0])

    return profiel_id, random.sample(categoryKeys, 4)

def data_storten(profiel, waarde, connectie, cursor, tabel, *rij):
    """
    Connect aan de database en loop door de verschillende waardes in de list en voeg ze die dan toe aan de table collums.
    Execute deze command en commit het naar de sql database.
    :param direction:, :param profile, :param list_value:, :param db:, :param cursor:, :param table:, :param *column:, :return:,
    """
    tomaat = True

    while tomaat:
        nieuwe_tabel = "INSERT IGNORE INTO " + tabel + " ("+rij[0]+", "+rij[1]+","+rij[2]+","+rij[3]+", "+rij[4] +") VALUES (%s, %s, %s, %s, %s)"
        sorteren = (str(profiel), str(waarde[0]), str(waarde[1]), str(waarde[2]), str(waarde[3]))
        cursor.execute(nieuwe_tabel, sorteren)
        connectie.commit()
        tomaat = False

def recommendation_engine():
    """
    Deze functie zet alles op gang & timed het process

    :return
    """


    #connect aan de database, verwijder eventuele bestaande tabellen en maakze opnieuw aan.
    connectie, cursor = connect_met_database()
    verwijder_tabellen(cursor)
    maak_tabellen_aan(cursor)
    profielen = profielen_db(cursor)
    for i in profielen:
        profiel_id, vergelijking = main_category_rec(cursor, i[0])
        data_storten(profiel_id, vergelijking, connectie, cursor, "main_category_rec", "id", "product1", "product2", "product3", "product4")


    database_sluiten(connectie, cursor)

    sys.exit(0)


recommendation_engine()
