import sys
import os
import argparse
import itertools
import re

class ItemsMap:
    user_id_to_item_list = []
    file_name = ""
    writer = ""
    user_id_to_int = {}
    dict_counter = 1
    item_id_to_int = {}
    item_id_dict_counter = 1

    def __init__(self, file_name):
        self.file_name = file_name
        self.writer = open("id_title_int_itemid_str_title_full.csv", "wb")

    def parse_item_block(self, data):

        asin = ""
        title = ""
        for item in data: #traverse list
            #clear item properties based on user input
            if 'ASIN:' in item:
                asin = item.partition(": ")[2].rstrip()
                if asin in self.item_id_to_int:
                    pass
                else:
                    self.item_id_to_int[asin] = self.item_id_dict_counter
                    self.item_id_dict_counter += 1           

            elif 'salesrank' in item:
                sales_rank = item.partition(": ")[2].rstrip()
            elif 'avg rating' in item:
                avg_rating = re.search(r'avg rating: [0-9]?.[0-9]?', item).group().partition(": ")[2].rstrip()
            elif 'title:' in item:
                title = item.partition(": ")[2].replace(",", " ").rstrip()
            elif 'group:' in item:
                group = item.partition(": ")[2].rstrip()
            elif 'similar:' in item:
                similar_list = item.partition(": ")
                similar_list = similar_list[2].partition(" ")
                similar_count = similar_list[0].rstrip()
                if int(similar_count) > 0:
                    similar_list = similar_list[2].rstrip()
                    similar_list = ' '.join(similar_list.split())
                else:
                    similar_list = 0
            elif 'cutomer:' in item:
                temp_list = item.split()
                date = temp_list[0]
                # date = re.search(r'[0-9]{,4}-[0-9]{1,2}-[0-9]{1,2}', item).group()
                customer_id = temp_list[2]
                user_rating = temp_list[4]
                vote = temp_list[6]
                helpful = temp_list[8].rstrip()
                # short print version
                # print("{0}, {1}, {2}".format(customer_id, asin, user_rating))

                # index: property
                # 0: customer id, 1: amazon product id, 2: user rating, 3: votes, 4: helpful,
                # 5: date, 6: sales rank, 7: title, 8: avg rating, 9: group, 10: similar item#
                # 11: similar item IDs (spae delimited)
                # entry = "{0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}, {11}".format(
                #     customer_id, asin, user_rating, vote, helpful, date, sales_rank, title, avg_rating,
                #     group, similar_count, similar_list)
                # print(entry)

                # self.writer.write(entry + "\n")

        if asin != "" and title != "":
            entry = "{0}, {1}".format(self.item_id_to_int[asin], title)
            print(entry)
            self.writer.write(entry + "\n")
    '''
    'ASIN: 0738700797\n', '  title: Candlemas: Feast of Flames\n', '  group: Book\n', '  
    salesrank: 168596\n', '  similar: 5  0738700827  1567184960  1567182836  0738700525  
    0738700940\n', '  categories: 2\n', '   |Books[283155]|Subjects[1000]|Religion & 
    Spirituality[22]|Earth-Based Religions[12472]|Wicca[12484]\n', '   
    |Books[283155]|Subjects[1000]|Religion & Spirituality[22]|Earth-Based Religions[12472]
    |Witchcraft[12486]\n', '  reviews: total: 12  downloaded: 12  avg rating: 4.5\n', '    
    2001-12-16  cutomer: A11NCO6YTE4BTJ  rating: 5  votes:   5  helpful:   4\n', '    
    2002-1-7  cutomer:  A9CQ3PLRNIR83  rating: 4  votes:   5  helpful:   5\n', '    
    2002-1-24  cutomer: A13SG9ACZ9O5IM  rating: 5  votes:   8  helpful:   8\n', '    
    2002-1-28  cutomer: A1BDAI6VEYMAZA  rating: 5  votes:   4  helpful:   4\n', '    
    2002-2-6  cutomer: A2P6KAWXJ16234  rating: 4  votes:  16  helpful:  16\n', '    
    2002-2-14  cutomer:  AMACWC3M7PQFR  rating: 4  votes:   5  helpful:   5\n', '    
    2002-3-23  cutomer: A3GO7UV9XX14D8  rating: 4  votes:   6  helpful:   6\n', '    
    2002-5-23  cutomer: A1GIL64QK68WKL  rating: 5  votes:   8  helpful:   8\n', '    
    2003-2-25  cutomer:  AEOBOF2ONQJWV  rating: 5  votes:   8  helpful:   5\n', '    
    2003-11-25  cutomer: A3IGHTES8ME05L  rating: 5  votes:   5  helpful:   5\n', '    
    2004-2-11  cutomer: A1CP26N8RHYVVO  rating: 1  votes:  13  helpful:   9\n', '    
    2005-2-7  cutomer:  ANEIANH0WAT9D  rating: 5  votes:   1  helpful:   1\n', '\n']
    '''
    def read_file(self):
        with open(self.file_name, "rb") as f:
            line = f.readline() #dump first line
            line = f.readline() #dump second line
            for key,group in itertools.groupby(f,self.isa_group_separator):
                # print(key,list(group))  # uncomment to see what itertools.groupby does.
                if not key:
                    self.parse_item_block(list(group))
        self.writer.close()

    def isa_group_separator(self, line):
        # return line == "\n\n"
        # if "ASIN" in line:
        if "Id:   " in line:
            return True
        return False

if __name__=="__main__":
    user_id_list = ItemsMap("amazon-meta.txt")
    user_id_list.read_file()


