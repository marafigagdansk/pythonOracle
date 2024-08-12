import cx_Oracle
import pandas as pd
from time import sleep



def connectDBD():

    settings = {
        "ORACLE_BENNER_HOST": "localhost",
        "ORACLE_BENNER_PORT": "1521",
        "ORACLE_BENNER_NAME": "XEPDB1"
    }
    conn = None 
    
    try:
        dsn = cx_Oracle.makedsn(
        "192.168.1.83",
        "1521",
        service_name="benntst6"
    )
        conn = cx_Oracle.connect(
            user="bennercorp",
            password="bennercorp",
            dsn= dsn  
        )
        cur = conn.cursor()           
        
    except Exception as e:
        print(f"Erro ao criar a tabela: {e}")

    if conn:
        df = pd.read_csv('C:\\Users\\vinic\\OneDrive\\Documentos\\pythonRemaster\\titanic (1).csv')
        df['Survived'] = df['Survived'].astype(str)

    try:
        cur.execute("""
                CREATE TABLE titanic (
                    Passenger_id NUMBER,
                    Survived VARCHAR2(1)
                )
            """)
        print("Tabela criada com sucesso.")

        for index, row in df.iterrows():
            cur.execute("""
                    INSERT INTO titanic (
                        Passenger_id, Survived
                    ) VALUES (
                        :1, :2
                    )
            """, tuple(row))
        conn.commit()
        print("Dados inseridos com sucesso.")

    except Exception as e:
        print(f"Erro ao inserir informações: {e}")

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close() 
    
    