
import mechanize
from bs4 import BeautifulSoup
import re
import urllib
global count,flag

count=1
flag=0

keyword=raw_input('Keyword Search:')
url=raw_input('URL:')
def googlesearch(keyword,url):

    parsed_key=urllib.quote_plus(keyword)           #parsing the keyword to convert to url
    #print(parsed_key)
    br = mechanize.Browser()
    br.set_handle_robots(False)
    br.set_handle_equiv(False)
    br.addheaders = [('User-agent', 'Mozilla/5.0')]
    br.open('http://www.google.com/')

    # do the query
    br.select_form(name='f')
    br.form['q'] = keyword                  # query
    data = br.submit()
    #print(data)
    soup = BeautifulSoup(data.read(),'html.parser')

    while flag!=1:
        global count,flag
        if(count==1):
            pass
        elif(count!=1):
            count=(count-count%10)+10
        for a in soup.select('.r a'):
            print(count)
            #print(a)    #scraped results  contains
            url_final=re.search('q=(.+?)&amp',str(a)).group(1)
            if (url_final==str(url)):
                print('Approx Rank Of Website:',count)
                print('Exact Between ',int(count-count%10),'to',int(count-count%10)+10)
                print(url_final)
                flag=1
                break
            count+=1
        if(flag!=1):
            page=str(int(count-count%10)+10)
            #print(page)
            print('page is ',int(page)/10)

            data=br.open('https://www.google.co.in/search?q='+parsed_key+'&dcr=0&biw=641&bih=615&ei=dfLAWpuBFMzovgTA1oOYDQ&start='+page+'&sa=N')

            soup = BeautifulSoup(data.read(),'html.parser')






        #print "Found the URL:", a['href']
googlesearch(keyword,url)

