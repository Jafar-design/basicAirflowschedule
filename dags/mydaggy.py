
from pandas import DataFrame
# import connection
from datetime import datetime
from airflow.decorators import task, dag
# from include import gtb_summary

now=datetime.now()



@dag(schedule='* * * * *', start_date=datetime(2022,10,19), catchup=False, tags=['myFirstdag'])
def writeToFile():
    
    print("Code zero, just before task decorator")
    @task
    def writeInsideTxt():
        # This code opens a new file if it doesn't exist before, and appends current timetsamp
        with open('wr1.txt', mode='a') as file:
            print("Starting to succeeed")
            file.write(now.strftime("%m/%d/%Y, %H:%M:%S") + '\n')
            print("Dag should succeed now")
    writeInsideTxt = writeInsideTxt()
            

writeToFile = writeToFile()










