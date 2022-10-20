import pymysql
import mysql.connector

def get_connection():
    conn = mysql.connector.connect(host="", 
    user="", 
    password="", 
    db="")
    cur = conn.cursor()
    return cur, conn