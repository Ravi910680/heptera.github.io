#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import colorama
from datetime import datetime
import mysql.connector


# In[18]:


url_info=[];

db_connect_site = mysql.connector.connect(
  host="hepteralogin.c86etdreadqr.us-east-2.rds.amazonaws.com",
  user="gautam910",
  password="Ravi91068",
    database="url_data"

)


db_connect_site_cursor = db_connect_site.cursor()


# In[3]:


db_connect_crw = mysql.connector.connect(
  host="hepteralogin.c86etdreadqr.us-east-2.rds.amazonaws.com",
  user="gautam910",
  password="Ravi91068",
    database="url_crw_data"

)

db_connect_crw_cursor = db_connect_crw.cursor()


# In[4]:


def sel_site_for_crw():
    
    db_connect_site_cursor.execute("SELECT * FROM url_connect where crw_flg=0 and status=1 LIMIT 1")
    myresult = db_connect_site_cursor.fetchall()
    for x in myresult:
        url_info.append(x)
    


# In[5]:


now = datetime.now()

formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')


# In[6]:


colorama.init()
GREEN = colorama.Fore.GREEN
GRAY = colorama.Fore.LIGHTBLACK_EX
RESET = colorama.Fore.RESET


# In[7]:


internal_urls = set()
external_urls = set()


# In[8]:


def is_valid(url):
    """
    Checks whether `url` is a valid URL.
    """
    parsed = urlparse(url)
    return bool(parsed.netloc) and bool(parsed.scheme)


# In[9]:


def get_all_website_links(url):
    """
    Returns all URLs that is found on `url` in which it belongs to the same website
    """
    # all URLs of `url`
    urls = set()
    # domain name of the URL without the protocol
    domain_name = urlparse(url).netloc
    soup = BeautifulSoup(requests.get(url).content, "html.parser")
    for a_tag in soup.findAll("a"):
        href = a_tag.attrs.get("href")
        if href == "" or href is None:
            # href empty tag
            continue
        # join the URL if it's relative (not absolute link)
        href = urljoin(url, href)
        parsed_href = urlparse(href)
        # remove URL GET parameters, URL fragments, etc.
        href = parsed_href.scheme + "://" + parsed_href.netloc + parsed_href.path
        if not is_valid(href):
            # not a valid URL
            continue
        if href in internal_urls:
            # already in the set
            continue
        if domain_name not in href:
            # external link
            if href not in external_urls:
                
                external_urls.add(href)
            continue
        
        urls.add(href)
        internal_urls.add(href)
    return urls


# In[10]:


total_urls_visited = 0

def crawl(url, max_urls=50):
    """
    Crawls a web page and extracts all links.
    You'll find all links in `external_urls` and `internal_urls` global set variables.
    params:
        max_urls (int): number of max urls to crawl, default is 30.
    """
    global total_urls_visited
    total_urls_visited += 1
    links = get_all_website_links(url)
    for link in links:
        if total_urls_visited > max_urls:
            break
        crawl(link, max_urls=max_urls)


# In[11]:


def isrt_all_link():
    str_ret_lst=[]
    for url in internal_urls:
        str_ret_lst.append((url,'name,date',formatted_date))
        
        
        
    return str_ret_lst


# In[ ]:





# In[19]:


if __name__ == "__main__":
    
    
    
    sel_site_for_crw()
    
    if(len(url_info)==1):
        crawl(url_info[0][1])
    
       
    
        sql = "INSERT IGNORE INTO `"+url_info[0][4]+"` (url, keyword,date) VALUES (%s, %s,%s)"
        str_of_isrt=isrt_all_link()
    
    
        db_connect_crw_cursor.executemany(sql, str_of_isrt)
    
        db_connect_crw.commit()
    


# In[ ]:





# In[ ]:




