
from pandas import DataFrame
import connection
from datetime import datetime
from airflow.decorators import task, dag
# from include import gtb_summary



now=datetime.now()
@dag(schedule='* * * * *', start_date=datetime(2022,10,19), catchup=False, tags=['myFirstdag'])
def writeToFile():
    
    @task
    def writeInstdeTxt():
        with open('output/wr1.txt', mode='a') as file:
            file.write(now.strftime("%m/%d/%Y, %H:%M:%S") + '\n')
            

writeToFile = writeToFile()










