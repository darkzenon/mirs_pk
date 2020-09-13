# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 00:05:09 2020

@author: denis
"""

import schedule
import time
import requests

import asyncio
import get_object_data

headers = {
        'Content-Type':'application/json; charset=utf-8',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0',
}

out_path = 'C:\\Users\\denis\\python\\hack_pkk\\presentaton\\kadast\\object\\'


class parser:
    
    def __init__(self, type_id = 1, sqot = 1, limit = 10):
        self.type_id = type_id
        self.sqo = ''
        self.text = ''
        self.sqot = sqot
        self.limit = limit        
        #self.skip = self.limit
        self.offset = 0
        self.buffer = []
        self.errors = []
        self.file = []
        self.headers = {
        'Content-Type':'application/json; charset=utf-8',
        'User-Agent':'Mozilla/1.0 (Windows 1.0; Win4; x4; rv:2.0) Gecko/201 Firefox/2.0',
                        }
		
			
    def ParseUrl_sqo(self, sqo, skip):
		
        self.sqo = sqo
        self.skip = skip
        
        for sqo_i in self.sqo:
            status_code = 0

            try:
                while status_code != 200:
                    #print ('Object: ' + str(sqo_i) + ', offset: ' + str(self.offset))
                    r = requests.get('https://pkk.rosreestr.ru/api/features/' + str(self.type_id) + '?sqo=' + str(sqo_i) + '&sqot='+ str(self.sqot) + '&limit=' + str(self.limit) + '&skip=' + str(self.skip), headers=headers, timeout=(60, 60))
                    status_code = r.status_code
                    time.sleep(1)
                    if len(r.json()['features']) != 0:
                        for elem in r.json()['features']:
                            self.buffer.append (elem)
                        self.offset += 1
                        if self.skip + self.limit > 200:
                            break
                        self.ParseUrl(self.sqo, self.skip + self.limit)
            except:
                self.errors.append('https://pkk.rosreestr.ru/api/features/' + str(self.type_id) + '?sqo=' + str(sqo_i) + '&sqot='+ str(self.sqot) + '&limit=' + str(self.limit) + '&skip=' + str(self.skip)) 
	
    def ParseUrl_text(self, text, skip):
		
        self.text = text
        self.skip = skip
        
        for text_i in self.text:
            status_code = 0

            try:
                while status_code != 200:
                    print ('Object: ' + str(text_i) + ', offset: ' + str(self.offset))
                    r = requests.get('https://pkk.rosreestr.ru/api/features/' + str(self.type_id) + '?text=' + str(text_i) + '&limit=' + str(self.limit) + '&skip=' + str(self.skip), headers=headers, timeout=(60, 60))
                    #print ('https://pkk.rosreestr.ru/api/features/' + str(self.type_id) + '?text=' + str(text_i) + '&limit=' + str(self.limit) + '&skip=' + str(self.skip))
                    status_code = r.status_code
                    time.sleep(1)
                    if len(r.json()['features']) != 0:
                        for elem in r.json()['features']:
                            self.buffer.append (elem)
                        self.offset += 1
                        if self.skip + self.limit > 9:
                            break
                        self.ParseUrl_text(self.text, self.skip + self.limit)
            except:
                self.errors.append('https://pkk.rosreestr.ru/api/features/' + str(self.type_id) + '?text=' + str(text_i) + '&limit=' + str(self.limit) + '&skip=' + str(self.skip)) 		
	 
    def output(self):
        print (self.buffer)

    def output_h(self):
        return self.buffer

    def save_buffer(self, path, file_name):
        
        id_fields = [x['attrs']['id'] + '\n' for x in self.buffer]
        with open(path + file_name, 'w') as file:
            file.writelines(id_fields)

def t():
    print("+")

    
    
def get_new_objects():
    
    #получаем список районов
    r_get = parser(3, 4, 10)
    r_get.ParseUrl_text(['59:*'], 0)
    raions = [x['attrs']['id'] for x in r_get.output_h()]
    
    #получаем список кварталов
    kvartals = []
    for raion in raions:
        k_get = parser(2, 1, 10)
        k_get.ParseUrl_text([ raion +':*'], 0)
        for x in k_get.output_h():
            kvartals.append(x['attrs']['id'])    

    #получаем список участков
    feilds = []
    for kvartal in kvartals[:5]:
        f_get = parser(1, 1, 10)
        f_get.ParseUrl_text([ kvartal +':*'], 0)
        for x in f_get.output_h():
            feilds.append(x['attrs']['id'])

# промежуточное сохранение    
#    with open( out_path + 'objects.txt', 'w') as file:
#        file.write("\n".join(feilds))


# сохраняем данные по каждому участку
    loop = asyncio.get_event_loop() 
    loop.create_task(get_object_data._main(feilds))


if __name__ == '__main__':
#    #schedule.every().monday.do(get_new_objects)
#    while True:
#        schedule.run_pending()
#        time.sleep(1)
    
    get_new_objects()
    