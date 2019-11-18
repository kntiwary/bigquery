from scrapper import scrap,log_error
from bs4 import BeautifulSoup

raw_html=scrap('http://example.webscraping.com')

soup = BeautifulSoup(raw_html, 'html5lib')
# print(soup.prettify())
table = soup.find('div', attrs = {'id':'results'})
print(table.prettify())
i=1
country={}
img_country= {}
for row in table.findAll('a'):
    country[i]=row.text
    # img_country[str(row.img['src'])] =row.text
    img_country[row.text] = str(row.img['src'])
    i+=1

for x in img_country.items():
    print(x,'\n')