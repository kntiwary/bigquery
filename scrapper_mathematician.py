from scrapper import scrap
from bs4 import BeautifulSoup
import csv

raw_html = scrap('http://www.fabpedigree.com/james/mathmen.htm')
html = BeautifulSoup(raw_html, 'html5lib')
# print(html.prettify())
# table = html.find('li')
# print(table.prettify())

mathematicians = []
for i, li in enumerate(html.select('li'), 101):
    m = {'id': i, 'Name': li.text.split('\n')[0]}
    mathematicians.append(m)

# print(mathematicians)


filename = 'mathematician.csv'
filename = 'mathematician1.csv'
with open(filename, 'w') as f:
    w = csv.DictWriter(f, ['id', 'Name'])
    w.writeheader()
    for mathematician in mathematicians:
        w.writerow(mathematician)
