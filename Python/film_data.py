import requests
import time
from lxml import etree


class DouBan(object):

    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/68.0.3440.106 Safari/537.36'}
        self.session = requests.session()
        self.file_path = '/home/lzh/文档/film.txt'
        self.proxies = {
            'http': 'http://219.141.153.41:80',
            'http': 'http://118.190.95.35:9001',
            'http': 'http://61.135.217.7:80',
            'http': 'http://118.190.95.43:9001',
            'http': 'http://101.236.51.35:8866'
        }

    def login(self):
        try:
            pass
        except Exception as e:
            print('login error: ', e.args)

    def get_data(self, url):
        try:
            final_url = url + str(0) + '&limit=20&sort=new_score&status=P'
            html = self.session.get(final_url, headers=self.headers, proxies=self.proxies)
            num = 0
            while html.status_code == 200:
                print('connect success')
                text = etree.HTML(html.text)
                stars = text.xpath('//*[@id="comments"]/div/div[2]/h3/span[2]/span[2]/@title')
                support = text.xpath('//*[@id="comments"]/div/div[2]/h3/span[1]/span/text()')
                print(stars)
                print(support)
                comments = text.xpath('//*[@id="comments"]/div/div[2]/p/span/text()')
                with open(self.file_path, 'a+') as f:
                    for i in range(len(stars)):
                        f.write(stars[i] + ' ' + support[i] + ' ' + comments[i] + '\n')
                num = num + 20
                url_next = url + str(num) + '&limit=20&sort=new_score&status=P'
                html = self.session.get(url_next, headers=self.headers, proxies=self.proxies)
                time.sleep(1)
        except Exception as e:
            print('get data error: ', e.args)


if __name__ == '__main__':
    spider = DouBan()
    simple_url = 'https://movie.douban.com/subject/26366496/comments?start='
    spider.get_data(simple_url)



