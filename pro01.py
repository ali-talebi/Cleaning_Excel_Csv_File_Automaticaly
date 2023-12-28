from threading import Thread
from multiprocessing import Process 
from concurrent.futures import as_completed 
import os 
import time 
import pandas as pd 
import datetime 
import csv




path_Folder_read_File = "test_vorodi" ### Input Folder File to Reading 
path_Folder_save_File = "test_khoroji" ### Output Folder File to Saving 



def Main_Function(name_file) : 


    import csv
    def find_delimiter(filename):
        sniffer = csv.Sniffer()
        with open(filename) as fp:
            delimiter = sniffer.sniff(fp.read(5000)).delimiter
        return delimiter
    


    


    def Change_Function(x) : 
        forbidens = ['!' , '~' , '&' , '%' , '$' , '"' , ":" , ";" , "," , "#" , "<", "?"  , '.' , '-' , "@" ]
        x = x.replace("(" , "_" ) 
        x = x.replace(")" , "_" ) 
        x = x.replace("/" , "_" ) 
        x = x.replace("/" , "_" ) 
        x = x.strip()
        x = x.replace(" " , "" )




        for char in forbidens : 
            if char in x : 
                x = x.replace(char , "_" ) 

        return x 


    time1 = time.time()
    name_file_split  , type_file = name_file.split(".")
    flag = 0 

    if type_file == "csv" : 
        
        data_frame = pd.read_csv(f"{path_Folder_read_File}/{name_file}" , header=0  )
        
        for column in data_frame.columns :
            data_frame[column] =  data_frame[column].apply(Change_Function)


        flag = 1 

        if flag == 1 : 
            with pd.ExcelWriter(f"{path_Folder_save_File}/{name_file_split}_clean_Data.xlsx" , engine="openpyxl"  ) as writer : 
                data_frame.to_excel(writer ,  index = False  )




    elif type_file == "xlsx" or type_file == "xls" : 
        tabs = pd.ExcelFile(f"{path_Folder_read_File}/{name_file}").sheet_names 
        for sheet_name in tabs : 
            #delimiter = find_delimiter(f"{path_Folder_read_File}/{name_file}" )
            data_frame = pd.read_excel(f"{path_Folder_read_File}/{name_file}" , header=0 ,  sheet_name = sheet_name  ) 
            for column in data_frame.columns :
                data_frame[column] =  data_frame[column].apply(Change_Function)


            flag = 1 


            if flag == 1 : 
            
                with pd.ExcelWriter(f"{path_Folder_save_File}/{name_file_split}_clean_Data_{sheet_name}.xlsx" , engine="openpyxl"  ) as writer : 
                    data_frame.to_excel(writer ,  index = False  )


    time2 = time.time()


    print("Time Duration is : " , time2 - time1 )
    print(f"File Name : {name_file} " ,  f" ************** System time ********** {datetime.datetime.now()} " )



        






if __name__ == "__main__" : 

    total_files = [] 
    for file in os.listdir(f"{path_Folder_read_File}") : 
        total_files.append(file)



    for index in range((len(total_files)//4 ) + 1 ) : 

        
        total_files2 = total_files[index*4 : index*4+4 ]
        tasks = []
        for filename in total_files2 : 
            task = Process(target=Main_Function , args=(f"{filename}" , ) )
            task.start()
            tasks.append(task)

        for p in tasks : 
            p.join()

        for p in tasks : 
            p.terminate() 







            






        
    
    



    

